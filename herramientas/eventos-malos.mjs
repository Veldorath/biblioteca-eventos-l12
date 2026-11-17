// E3, paso 1 · Publica a proposito un evento malo de cada clase, mas un duplicado, para ver el .dlt sin el
// simulador. Corre en el equipo (no en el contenedor), desde biblioteca-eventos y despues de `npm ci`:
//   node herramientas/eventos-malos.mjs                 en biblioteca.prestamos
//   node herramientas/eventos-malos.mjs lecturas        en biblioteca.lecturas
//   KAFKA_BROKERS=10.0.21.11:9092 node herramientas/eventos-malos.mjs     otro cluster (por defecto, el local)
//
// Lo que tiene que pasar, por cada grupo que lee ese topic (prestamos: panel e historial; lecturas: panel y
// alertas): 4 mensajes en <topic>.dlt, uno por x-motivo, y el duplicado confirmado sin escribir. El evento
// bueno es un prestamo.devuelto o un capitulo.terminado de mentira: no cambia ninguna cifra del panel ni del
// historial.
import { randomUUID } from 'node:crypto';
import { Producer, ProduceAcks, compatibilityPartitioner, stringSerializers } from '@platformatic/kafka';

const cual = process.argv[2] ?? 'prestamos';
if (!['prestamos', 'lecturas'].includes(cual)) {
  console.error('uso: node herramientas/eventos-malos.mjs [prestamos|lecturas]');
  process.exit(1);
}
const topic = `biblioteca.${cual}`;
const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092,localhost:9094,localhost:9096').split(',');

const usuarioSub = 'a4c8e1f2-3b5d-4a7e-9c01-2f6b8d4e5a90';
const libroId = '7';
const bueno = () => ({
  eventoId: randomUUID(),
  tipo: cual === 'prestamos' ? 'prestamo.devuelto' : 'capitulo.terminado',
  version: 1,
  emitidoEn: new Date().toISOString(),
  libroId,
  usuarioSub,
  datos: cual === 'prestamos' ? { prestamoId: `prueba-e3-${Date.now()}` } : { sesionId: randomUUID(), capitulo: 1 },
});

const sinEventoId = bueno();
delete sinEventoId.eventoId;
const repetido = bueno();

// [que es, value, x-motivo esperado]
const casos = [
  ['JSON cortado a la mitad', JSON.stringify(bueno()).slice(0, 80), 'json-invalido'],
  ['sin eventoId', JSON.stringify(sinEventoId), 'falta-eventoId'],
  ['version 99', JSON.stringify({ ...bueno(), version: 99 }), 'version-desconocida'],
  ['eventoId que no es uuid', JSON.stringify({ ...bueno(), eventoId: 'no-soy-un-uuid' }), 'dto-invalido'],
  ['valido', JSON.stringify(repetido), 'no va al .dlt'],
  ['el mismo otra vez (duplicado)', JSON.stringify(repetido), 'no va al .dlt'],
];

const productor = new Producer({
  clientId: 'biblioteca-eventos-malos',
  bootstrapBrokers: brokers,
  serializers: stringSerializers,
  idempotent: true,
  acks: ProduceAcks.ALL,
  partitioner: compatibilityPartitioner,
  autocreateTopics: false,
});

try {
  for (const [que, value, esperado] of casos) {
    const evento = (() => {
      try {
        return JSON.parse(value);
      } catch {
        return {};
      }
    })();
    const headers = { 'content-type': 'application/json', 'x-prueba': 'eventos-malos' };
    if (evento.eventoId) headers['x-evento-id'] = evento.eventoId;
    const r = await productor.send({
      messages: [{ topic, key: cual === 'prestamos' ? usuarioSub : libroId, value, headers }],
    });
    const o = r.offsets?.[0];
    console.log(`${topic}[${o?.partition}]@${o?.offset}  ${que.padEnd(30)} -> ${esperado}`);
  }
  console.log(`listo: 6 mensajes en ${topic}. Mira ${topic}.dlt (4 por cada grupo que lee ${topic}).`);
} finally {
  await productor.close();
}
