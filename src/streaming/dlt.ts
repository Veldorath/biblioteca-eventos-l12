import { Producer, ProduceAcks, compatibilityPartitioner, stringSerializers } from '@platformatic/kafka';
import type { Mensaje, MotivoInvalido } from './contrato.js';

/** Por que un evento termina en el .dlt: no cumple el contrato, o Postgres no respondio en ningun reintento. */
export type MotivoDlt = MotivoInvalido | 'postgres-no-responde';

/** El .dlt de un topic: mismo nombre con el sufijo .dlt. Los crea biblioteca-plataforma/kafka/topics.sh. */
export const topicDlt = (topic: string): string => `${topic}.dlt`;

// Un header de Kafka no tiene limite practico, pero el detalle de class-validator puede ser largo: se corta.
const LARGO_DETALLE = 500;

/**
 * Los headers del mensaje desviado: los del original, tal cual llegaron (x-evento-id, x-correlation-id,
 * content-type...), mas de donde salio y por que. Con x-topic-origen, x-particion y x-offset se puede ir a
 * buscar el original; con x-grupo se sabe que consumer group lo desvio (el mismo evento malo llega una vez por
 * cada grupo que lo lee).
 */
export function cabecerasDlt(m: Mensaje, grupo: string, motivo: MotivoDlt, detalle: string): Map<string, string> {
  const cabeceras = new Map(m.headers);
  cabeceras.set('x-motivo', motivo);
  cabeceras.set('x-grupo', grupo);
  cabeceras.set('x-topic-origen', m.topic);
  cabeceras.set('x-particion', String(m.partition));
  cabeceras.set('x-offset', String(m.offset));
  cabeceras.set('x-detalle', detalle.slice(0, LARGO_DETALLE));
  return cabeceras;
}

/**
 * Publica en el .dlt lo que un grupo no puede procesar. Lo publica la aplicacion: la DLQ que trae Kafka 4.4 en el
 * broker es solo para share groups, y aqui se usan consumer groups.
 *
 * acks=all e idempotente, como los demas productores: un mensaje que se confirma en el origen y se pierde en el
 * .dlt seria un evento perdido sin rastro. Mismo particionador que el resto del sistema (murmur2, como Java) y la
 * key del original: con el mismo numero de particiones, el desviado cae en la misma particion que el original.
 */
export class PublicadorDlt {
  private readonly productor: Producer<string, string, string, string>;

  constructor(brokers: string[], clientId: string) {
    this.productor = new Producer({
      clientId,
      bootstrapBrokers: brokers,
      serializers: stringSerializers,
      idempotent: true,
      acks: ProduceAcks.ALL,
      retries: 10,
      partitioner: compatibilityPartitioner,
      autocreateTopics: false,
    });
  }

  /** Devuelve el topic y la particion donde quedo. Si Kafka no confirma, lanza: el original no se confirma. */
  async desviar(m: Mensaje, grupo: string, motivo: MotivoDlt, detalle: string): Promise<string> {
    const topic = topicDlt(m.topic);
    const r = await this.productor.send({
      messages: [{ topic, key: m.key || undefined, value: m.value, headers: cabecerasDlt(m, grupo, motivo, detalle) }],
    });
    const destino = r.offsets?.[0];
    return destino ? `${topic}[${destino.partition}]@${destino.offset}` : topic;
  }

  async cerrar(): Promise<void> {
    await this.productor.close();
  }
}
