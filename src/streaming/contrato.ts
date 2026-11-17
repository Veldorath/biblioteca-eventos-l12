import type { Message } from '@platformatic/kafka';
import { plainToInstance } from 'class-transformer';
import { validateSync, type ValidationError } from 'class-validator';

export type Mensaje = Message<string, string, string, string>;

/**
 * Por que un evento no cumple el contrato. Va en el header x-motivo del .dlt (E3, paso 1):
 * - json-invalido: el value no es JSON.
 * - falta-<campo>: falta un campo del sobre (eventoId, tipo, version, emitidoEn, ...).
 * - version-desconocida: viene una version que este consumidor no sabe leer (todo lo que no sea 1).
 * - dto-invalido: es JSON y trae todo, pero algo no valida (un uuid mal formado, un tipo que no existe...).
 */
export type MotivoInvalido = 'json-invalido' | `falta-${string}` | 'version-desconocida' | 'dto-invalido';

/** Un mensaje que no cumple el contrato: no se reintenta, va al .dlt con su motivo y se confirma. */
export class EventoInvalido extends Error {
  constructor(
    readonly motivo: MotivoInvalido,
    detalle: string,
  ) {
    super(detalle);
  }
}

/**
 * El contrato de un topic: convierte el value del mensaje en el DTO validado, o lanza EventoInvalido con el
 * motivo. No sabe nada de Kafka mas alla del value, ni de Postgres.
 */
export class Contrato<T extends object> {
  constructor(private readonly dto: new () => T) {}

  validar(m: Mensaje): T {
    let crudo: unknown;
    try {
      crudo = JSON.parse(m.value);
    } catch (error) {
      throw new EventoInvalido('json-invalido', `no es un JSON de evento (${(error as Error).message})`);
    }
    if (crudo === null || typeof crudo !== 'object' || Array.isArray(crudo)) {
      throw new EventoInvalido('dto-invalido', 'el value es JSON, pero no es un objeto');
    }
    const evento = plainToInstance(this.dto, crudo);
    const errores = validateSync(evento, { whitelist: true, forbidNonWhitelisted: true });
    if (errores.length === 0) return evento;

    const detalle = motivos(errores).join('; ');
    // Primero lo que falta: un evento sin eventoId no se puede ni deduplicar. Los errores vienen en el orden en
    // que el DTO declara sus campos (eventoId, tipo, version, emitidoEn, ...).
    const faltante = errores.find((e) => e.value === undefined || e.value === null);
    if (faltante) throw new EventoInvalido(`falta-${faltante.property}`, detalle);
    if (errores.some((e) => e.property === 'version')) throw new EventoInvalido('version-desconocida', detalle);
    throw new EventoInvalido('dto-invalido', detalle);
  }
}

// "version (equals)", "datos.desde (matches)": que propiedad falla y que regla incumple.
function motivos(errores: ValidationError[], ruta = ''): string[] {
  return errores.flatMap((e) => [
    ...(e.constraints ? [`${ruta}${e.property} (${Object.keys(e.constraints).join(', ')})`] : []),
    ...motivos(e.children ?? [], `${ruta}${e.property}.`),
  ]);
}
