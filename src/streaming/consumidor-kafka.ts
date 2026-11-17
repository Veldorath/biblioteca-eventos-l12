import { Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { setTimeout as esperar } from 'node:timers/promises';
import { ConfigService } from '@nestjs/config';
import {
  Consumer,
  MessagesStream,
  MessagesStreamFallbackModes,
  MessagesStreamModes,
  stringDeserializers,
} from '@platformatic/kafka';
import { Contrato, EventoInvalido, type Mensaje } from './contrato.js';
import { PublicadorDlt, type MotivoDlt } from './dlt.js';

/** Lo que devuelve la clase de negocio: si el evento cambio algo, si ya estaba, o si a este grupo no le sirve. */
export type Resultado = 'nuevo' | 'duplicado' | 'ignorado';

/** La clase de negocio de un grupo: recibe el evento ya validado y decide que hacer con el. No conoce Kafka. */
export interface Procesador<T> {
  procesar(evento: T): Promise<Resultado>;
}

export type OpcionesGrupo<T extends object> = {
  grupo: string;
  topics: string[];
  dto: new () => T;
  // true: lee siempre desde el offset 0 (el catalogo se reconstruye entero en cada arranque).
  desdeElPrincipio?: boolean;
};

// Si Postgres no responde: 3 reintentos, esperando 1, 2 y 4 s (7 s en total). Despues, al .dlt.
export const ESPERAS_REINTENTO_MS = [1000, 2000, 4000];
const CADA_RESUMEN_MS = 10_000;

/**
 * La parte de Kafka de un consumer group: conexion, suscripcion, commit y el .dlt. Cada grupo es una subclase de
 * una linea con su nombre, sus topics y su clase de negocio.
 *
 * Ningun evento detiene al consumidor (E3, paso 1):
 * - No cumple el contrato (JSON roto, falta un campo, version distinta de 1, DTO invalido): al .dlt con su
 *   x-motivo, se registra y se confirma. Reintentar no lo arregla.
 * - Postgres no responde: 3 reintentos con espera creciente; si sigue sin responder, al .dlt con
 *   x-motivo postgres-no-responde y se confirma. El evento esta bien: se puede reprocesar desde el .dlt.
 * - Duplicado: la clase de negocio no escribe (UNIQUE) y se confirma. No va al .dlt.
 * El orden que no pierde: leer, validar, procesar (o desviar) y RECIEN despues confirmar el offset.
 */
export abstract class ConsumidorKafka<T extends object> implements OnModuleInit, OnModuleDestroy {
  protected readonly log: Logger;
  private readonly consumidor: Consumer<string, string, string, string>;
  private readonly dlt: PublicadorDlt;
  private readonly contrato: Contrato<T>;
  private flujo?: MessagesStream<string, string, string, string>;
  private reloj?: NodeJS.Timeout;
  private cerrando = false;
  private readonly cuenta = { nuevos: 0, duplicados: 0, ignorados: 0, invalidos: 0, sinPostgres: 0 };
  private ultimoResumen = '';

  protected constructor(
    config: ConfigService,
    private readonly opciones: OpcionesGrupo<T>,
    private readonly negocio: Procesador<T>,
  ) {
    this.log = new Logger(opciones.grupo);
    this.contrato = new Contrato(opciones.dto);
    const brokers = config.getOrThrow<string>('KAFKA_BROKERS').split(',');
    this.consumidor = new Consumer({
      groupId: opciones.grupo,
      clientId: 'biblioteca-eventos',
      bootstrapBrokers: brokers,
      deserializers: stringDeserializers,
    });
    this.dlt = new PublicadorDlt(brokers, `biblioteca-eventos-dlt-${opciones.grupo}`);
  }

  async onModuleInit(): Promise<void> {
    this.flujo = await this.consumidor.consume({
      topics: this.opciones.topics,
      autocommit: false,
      mode: this.opciones.desdeElPrincipio ? MessagesStreamModes.EARLIEST : MessagesStreamModes.COMMITTED,
      fallbackMode: MessagesStreamFallbackModes.EARLIEST,
    });
    this.log.log(`grupo ${this.opciones.grupo} leyendo ${this.opciones.topics.join(' y ')}`);
    this.reloj = setInterval(() => this.resumir(), CADA_RESUMEN_MS);
    void this.leer(); // sin await: si no, Nest no termina de arrancar
  }

  private async leer(): Promise<void> {
    try {
      for await (const m of this.flujo!) {
        const donde = `${m.topic}[${m.partition}]@${m.offset}`;
        let evento: T;
        try {
          evento = this.contrato.validar(m);
        } catch (error) {
          if (!(error instanceof EventoInvalido)) throw error;
          this.cuenta.invalidos++;
          await this.desviar(m, donde, error.motivo, error.message);
          await m.commit(); // reintentar no lo arregla: queda en el .dlt
          continue;
        }
        const resultado = await this.procesarConReintentos(evento, donde);
        if (resultado === undefined) return; // se esta cerrando: sin commit, otro lo relee
        if (resultado instanceof Error) {
          this.cuenta.sinPostgres++;
          await this.desviar(m, donde, 'postgres-no-responde', resultado.message);
        }
        if (resultado === 'nuevo') this.cuenta.nuevos++;
        if (resultado === 'ignorado') this.cuenta.ignorados++;
        if (resultado === 'duplicado') {
          this.cuenta.duplicados++;
          this.log.warn(`${donde} DUPLICADO: ya estaba procesado, no suma`);
        }
        await m.commit(); // recien ahora: lo escrito ya esta en Postgres, o el evento ya esta en el .dlt
      }
    } catch (error) {
      if (!this.cerrando) this.log.error(`el consumidor se detuvo: ${(error as Error).message}`);
    }
  }

  // Si el .dlt no confirma (Kafka caido), lanza: el original no se confirma y el consumidor se detiene.
  private async desviar(m: Mensaje, donde: string, motivo: MotivoDlt, detalle: string): Promise<void> {
    const destino = await this.dlt.desviar(m, this.opciones.grupo, motivo, detalle);
    this.log.warn(`${donde} AL .DLT (${motivo}) -> ${destino}: ${detalle}`);
  }

  /**
   * Procesa el evento. Si falla (Postgres no responde), lo reintenta 3 veces esperando 1, 2 y 4 s. Devuelve el
   * resultado, el ultimo error si se agotaron los reintentos, o undefined si el servicio se esta cerrando.
   */
  private async procesarConReintentos(evento: T, donde: string): Promise<Resultado | Error | undefined> {
    for (let intento = 0; !this.cerrando; intento++) {
      try {
        return await this.negocio.procesar(evento);
      } catch (error) {
        const causa = error instanceof Error ? error : new Error(String(error));
        if (intento === ESPERAS_REINTENTO_MS.length) {
          this.log.error(`${donde} sin Postgres tras ${intento} reintentos: ${causa.message}`);
          return causa;
        }
        const espera = ESPERAS_REINTENTO_MS[intento];
        this.log.error(
          `${donde} no se pudo escribir, reintento ${intento + 1} de ${ESPERAS_REINTENTO_MS.length} en ${espera / 1000} s: ${causa.message}`,
        );
        await esperar(espera);
      }
    }
    return undefined;
  }

  // Una linea cada 10 s, solo si cambio algo: lo que se ve en `docker compose logs eventos`.
  private resumir(): void {
    const { nuevos, duplicados, ignorados, invalidos, sinPostgres } = this.cuenta;
    const texto =
      `desde el arranque: ${nuevos} nuevos, ${duplicados} duplicados, ${ignorados} ignorados, ` +
      `${invalidos + sinPostgres} al .dlt (${invalidos} invalidos, ${sinPostgres} sin postgres)`;
    if (texto === this.ultimoResumen || nuevos + duplicados + ignorados + invalidos + sinPostgres === 0) return;
    this.ultimoResumen = texto;
    this.log.log(texto);
  }

  // Con app.enableShutdownHooks() en main.ts. close() deja el grupo al tiro: el siguiente arranque, o el
  // reset de offsets, no espera a que venza la sesion del consumidor.
  async onModuleDestroy(): Promise<void> {
    this.cerrando = true;
    clearInterval(this.reloj);
    this.resumir();
    await this.flujo?.close();
    await this.consumidor.close();
    await this.dlt.cerrar();
  }
}
