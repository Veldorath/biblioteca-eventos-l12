import 'reflect-metadata';
import { Contrato, EventoInvalido, type Mensaje } from './contrato.js';
import { cabecerasDlt } from './dlt.js';
import { EventoDto } from './evento.dto.js';

// Un prestamo.creado valido, como los del simulador.
const valido = {
  eventoId: '0f8c2a4e-1b2c-4d3e-8f90-123456789abc',
  tipo: 'prestamo.creado',
  version: 1,
  emitidoEn: '2026-11-07T00:01:58.191Z',
  libroId: '18',
  usuarioSub: '4151313c-10f8-4610-b25b-d58abc5c7f2c',
  datos: { prestamoId: 'p-1', desde: '2026-11-06', hasta: '2026-11-13' },
};

const mensaje = (value: string): Mensaje =>
  ({
    topic: 'biblioteca.prestamos',
    partition: 1,
    offset: 33n,
    key: valido.usuarioSub,
    value,
    headers: new Map([['x-evento-id', valido.eventoId], ['content-type', 'application/json']]),
  }) as unknown as Mensaje;

const contrato = new Contrato(EventoDto);

function motivoDe(value: string): string {
  try {
    contrato.validar(mensaje(value));
  } catch (error) {
    if (error instanceof EventoInvalido) return error.motivo;
    throw error;
  }
  return 'valido';
}

const sin = (campo: keyof typeof valido) => {
  const copia: Record<string, unknown> = { ...valido };
  delete copia[campo];
  return JSON.stringify(copia);
};

describe('Contrato: el x-motivo de cada evento invalido', () => {
  it('un evento valido pasa', () => {
    expect(motivoDe(JSON.stringify(valido))).toBe('valido');
  });

  it('JSON cortado a la mitad: json-invalido', () => {
    expect(motivoDe(JSON.stringify(valido).slice(0, 120))).toBe('json-invalido');
  });

  it.each(['eventoId', 'tipo', 'version', 'emitidoEn', 'libroId', 'usuarioSub'] as const)(
    'falta %s: falta-%s',
    (campo) => {
      expect(motivoDe(sin(campo))).toBe(`falta-${campo}`);
    },
  );

  it('version 99: version-desconocida', () => {
    expect(motivoDe(JSON.stringify({ ...valido, version: 99 }))).toBe('version-desconocida');
  });

  it('falta un campo y ademas la version es otra: manda lo que falta', () => {
    const { eventoId: _, ...resto } = { ...valido, version: 99 };
    expect(motivoDe(JSON.stringify(resto))).toBe('falta-eventoId');
  });

  it('eventoId que no es uuid, tipo desconocido o datos mal formados: dto-invalido', () => {
    expect(motivoDe(JSON.stringify({ ...valido, eventoId: 'abc' }))).toBe('dto-invalido');
    expect(motivoDe(JSON.stringify({ ...valido, tipo: 'prestamo.perdido' }))).toBe('dto-invalido');
    expect(motivoDe(JSON.stringify({ ...valido, datos: { prestamoId: 'p-1', desde: 'ayer', hasta: '2026-11-13' } }))).toBe(
      'dto-invalido',
    );
  });

  it('JSON que no es un objeto: dto-invalido', () => {
    expect(motivoDe('[1, 2]')).toBe('dto-invalido');
    expect(motivoDe('null')).toBe('dto-invalido');
  });
});

describe('cabecerasDlt', () => {
  it('conserva los headers del original y agrega motivo, grupo, topic, particion y offset', () => {
    const c = cabecerasDlt(mensaje('{'), 'biblioteca-panel', 'json-invalido', 'no es un JSON de evento');
    expect(Object.fromEntries(c)).toEqual({
      'x-evento-id': valido.eventoId,
      'content-type': 'application/json',
      'x-motivo': 'json-invalido',
      'x-grupo': 'biblioteca-panel',
      'x-topic-origen': 'biblioteca.prestamos',
      'x-particion': '1',
      'x-offset': '33',
      'x-detalle': 'no es un JSON de evento',
    });
  });
});
