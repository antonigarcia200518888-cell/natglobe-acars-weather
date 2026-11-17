import { randomUUID } from 'node:crypto';

export const DOCUMENT_SCHEMA = `CREATE TABLE IF NOT EXISTS pilot_flight_documents (
      flight_id TEXT NOT NULL REFERENCES booking_requests(id) ON DELETE CASCADE,
      id UUID NOT NULL UNIQUE, slot INTEGER NOT NULL CHECK (slot BETWEEN 1 AND 10),
      name TEXT NOT NULL, mime TEXT NOT NULL, content BYTEA NOT NULL CHECK (octet_length(content) BETWEEN 1 AND 2097152),
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY(flight_id,slot)
    )`;
const TYPES = new Set(['operational','private','training']);
const fail = (status, message) => Object.assign(new Error(message), {status});

export function managementUpdate(input, previous = {}) {
  if (!TYPES.has(input?.category)) throw fail(400,'Select a flight category.');
  const result = {...previous, category:input.category};
  if (input.category === 'training') {
    result.training = {...previous.training};
    for (const [key,limit] of [['instructor',100],['exercises',1000],['progress',1000]]) {
      if (input[key] === undefined) continue;
      if (typeof input[key] !== 'string' || input[key].length > limit) throw fail(400,'Invalid training field: '+key);
      result.training[key] = input[key].trim();
    }
  }
  return result;
}

export function validDocument(data, mime) {
  if (!Buffer.isBuffer(data) || data.length < 8 || data.length > 2097152) return false;
  return (mime === 'application/pdf' && data.subarray(0,5).toString() === '%PDF-')
    || (mime === 'image/png' && data.subarray(0,8).toString('hex') === '89504e470d0a1a0a')
    || (mime === 'image/jpeg' && data.subarray(0,3).toString('hex') === 'ffd8ff');
}

export function mountFlightManagement(app,{requirePilotAccess,requireFlightCrew,ready,pool,lookup,raw}) {
  const base='/api/booking-ops/requests/:id';
  const guard = async (req,res,next) => {
    if (!requireFlightCrew(req,res)) return;
    if (!['HEAD','GET'].includes(req.method) && req.get('Sec-Fetch-Site') === 'cross-site') return res.sendStatus(403);
    await ready;
    req.flightFile=lookup(req.params.id);
    res.set('Cache-Control','private, no-store');
    if (!req.flightFile) return res.status(404).json({error:'Flight file not found.'});
    next();
  };
  const handle = fn => async (req,res) => {
    try { await fn(req,res); } catch(e) {
      if (!e.status) console.error('Flight management:', e.code || e.name);
      res.status(e.status || 503).json({error:e.status ? e.message : 'Storage unavailable. Retry before closing this flight.'});
    }
  };
  app.patch(base+'/management',requirePilotAccess,guard,handle(async(req,res)=>{
    const management=managementUpdate(req.body,req.flightFile.management);
    if (pool()) {
      const result=await pool().query("UPDATE booking_requests SET payload=jsonb_set(payload,'{management}',$2::jsonb,true) WHERE id=$1",[req.params.id,JSON.stringify(management)]);
      if (!result.rowCount) throw fail(409,'Flight file no longer exists.');
    }
    req.flightFile.management=management;
    res.json({management});
  }));
  const docs=base+'/documents';
  app.use(docs,requirePilotAccess,guard,(req,res,next)=>pool()?next():res.status(503).json({error:'Document storage is offline. No attachments can be saved or retrieved.'}));
  app.get(docs,handle(async(req,res)=>{
    const {rows}=await pool().query('SELECT id,name,mime,created_at,octet_length(content) AS size FROM pilot_flight_documents WHERE flight_id=$1 ORDER BY created_at DESC',[req.params.id]);
    res.json({documents:rows});
  }));
  app.post(docs,raw,handle(async(req,res)=>{
    const mime=req.get('Content-Type')?.split(';')[0];
    if (!validDocument(req.body,mime)) throw fail(415,'Choose a PDF, JPEG or PNG file no larger than 2 MB.');
    const name=String(req.query.name || 'Document').split(/[\\/]/).pop().replace(/[^\p{L}\p{N} ._()-]/gu,'_').trim().slice(0,120) || 'Document';
    const id=randomUUID();
    const result=await pool().query(`INSERT INTO pilot_flight_documents(flight_id,id,slot,name,mime,content)
      SELECT $1,$2,n,$3,$4,$5 FROM generate_series(1,10) n WHERE NOT EXISTS
      (SELECT 1 FROM pilot_flight_documents WHERE flight_id=$1 AND slot=n) ORDER BY n LIMIT 1 ON CONFLICT DO NOTHING`,[req.params.id,id,name,mime,req.body]);
    if (!result.rowCount) throw fail(409,'No document slot available. Refresh the list; the limit is ten per flight.');
    res.status(201).json({id});
  }));
  const validId=req=>{if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(req.params.doc)) throw fail(404,'Document not found.'); return [req.params.id,req.params.doc];};
  app.get(docs+'/:doc',handle(async(req,res)=>{
    const {rows:[doc]}=await pool().query('SELECT name,mime,content FROM pilot_flight_documents WHERE flight_id=$1 AND id=$2',validId(req));
    if(!doc) throw fail(404,'Document not found.');
    res.set({'Content-Type':doc.mime,'X-Content-Type-Options':'nosniff','Content-Security-Policy':"sandbox; default-src 'none'",'Content-Disposition':(req.query.view==='1'?'inline':'attachment')+"; filename*=UTF-8''"+encodeURIComponent(doc.name)}).send(doc.content);
  }));
  app.delete(docs+'/:doc',handle(async(req,res)=>{
    const result=await pool().query('DELETE FROM pilot_flight_documents WHERE flight_id=$1 AND id=$2',validId(req));
    if (!result.rowCount) throw fail(404,'Document not found.');
    res.sendStatus(204);
  }));
}
