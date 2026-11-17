import {XMLParser,XMLValidator} from 'fast-xml-parser';
import {createHash} from 'node:crypto';

export const MAX_IMPORT_BYTES=8*1024*1024;
const MAX_POINTS=100000;
const list=v=>v==null?[]:Array.isArray(v)?v:[v];
const text=v=>typeof v==='string'||typeof v==='number'?String(v).trim():'';
const bad=message=>{throw Object.assign(new Error(message),{status:422});};
const number=(v,min,max,label,optional=false)=>{
  if(v==null || String(v).trim()===''){if(optional)return null;bad('Missing '+label);}
  const n=Number(v);if(!Number.isFinite(n)||n<min||n>max)bad('Invalid '+label);return n;
};
export function validUtc(v){
  if(typeof v!=='string'||!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,3})?)?Z$/.test(v))return false;
  const full=v.length===17?v.slice(0,-1)+':00Z':v;
  return Number.isFinite(Date.parse(full)) && new Date(full).toISOString().slice(0,19)===full.slice(0,19);
}
function instant(v,epoch=false){
  if(v==null || v==='')return null;
  if(!epoch && !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(v))bad('Timestamp needs an explicit UTC offset.');
  if(!epoch){const date=v.slice(0,10),day=new Date(date+'T00:00:00Z');if(!Number.isFinite(+day)||day.toISOString().slice(0,10)!==date)bad('Invalid calendar date.');if(Number(v.slice(11,13))>23||Number(v.slice(14,16))>59||Number(v.slice(17,19))>59)bad('Invalid timestamp clock.');}
  const n=epoch?number(v,0,4102444800,'epoch timestamp')*1000:Date.parse(v);
  if(!Number.isFinite(n)||n<0||n>4102444800000)bad('Invalid timestamp.');
  return new Date(n).toISOString();
}
function xml(raw){
  if(/<!\s*(?:DOCTYPE|ENTITY)/i.test(raw))bad('DTD and entity declarations are not permitted.');
  const valid=XMLValidator.validate(raw);if(valid!==true)bad('Malformed XML: '+valid.err.msg);
  return new XMLParser({ignoreAttributes:false,attributeNamePrefix:'@',removeNSPrefix:true,parseTagValue:false,parseAttributeValue:false,processEntities:false,maxNestedTags:32}).parse(raw);
}
function nodes(root,key){
  const found=[],stack=[root];
  while(stack.length){const obj=stack.pop();if(!obj || typeof obj!=='object')continue;
    for(const [k,v] of Object.entries(obj)){if(k===key)found.push(...list(v));else if(typeof v==='object')stack.push(...list(v).reverse());}
  }return found;
}
export function distanceNm(a,b){
  const r=Math.PI/180,dlat=(b.lat-a.lat)*r,dlon=(b.lon-a.lon)*r;
  return 3440.065*2*Math.asin(Math.min(1,Math.sqrt(Math.sin(dlat/2)**2+Math.cos(a.lat*r)*Math.cos(b.lat*r)*Math.sin(dlon/2)**2)));
}
function point(lat,lon,alt,time,speed=null,course=null,segment=0){
  return {lat:number(lat,-90,90,'latitude'),lon:number(lon,-180,180,'longitude'),
    altitudeFt:alt==null?null:number(alt,-3000,100000,'recorded altitude'),time,
    speedKt:speed==null?null:number(speed,0,2000,'ground speed'),course:course==null?null:number(course,0,360,'true course'),segment};
}
function csvRows(raw){
  const out=[];let row=[],cell='',quoted=false;
  for(let i=0;i<raw.length;i++){
    const c=raw[i];
    if(c==='"'){if(quoted&&raw[i+1]==='"'){cell+='"';i++;}else if(!quoted&&cell.trim())bad('Invalid CSV quoting.');else quoted=!quoted;}
    else if(c===','&&!quoted){row.push(cell.trim());cell='';}
    else if((c==='\n'||c==='\r')&&!quoted){if(c==='\r'&&raw[i+1]==='\n')i++;row.push(cell.trim());out.push(row);row=[];cell='';}
    else cell+=c;
    if(out.length>MAX_POINTS+80)bad('Track exceeds 100,000 points.');
  }
  if(quoted)bad('Unclosed CSV field.');if(cell||row.length){row.push(cell.trim());out.push(row);}return out;
}
function parseTrack(raw,name){
  const points=[],warnings=[];let format,altitudeReference='Not supplied';
  const push=p=>{if(points.length>=MAX_POINTS)bad('Track exceeds 100,000 points.');points.push(p);};
  if(/\.csv$/i.test(name) || !raw.trim().startsWith('<')){
    format='ForeFlight CSV';const rows=csvRows(raw);
    const header=rows.findIndex((r,i)=>i<60 && r.map(c=>c.toLowerCase()).includes('latitude') && r.map(c=>c.toLowerCase()).includes('longitude'));
    if(header<0)bad('CSV needs the documented Latitude and Longitude column headings.');
    const cols=rows[header].map(c=>c.toLowerCase());
    if(new Set(cols).size!==cols.length)bad('Duplicate CSV column headings.');
    const at=(r,key)=>r[cols.indexOf(key)];
    for(const r of rows.slice(header+1)){if(r.every(v=>!v))continue;
      const alt=number(at(r,'altitude'),-3000,100000,'altitude',true);
      push(point(at(r,'latitude'),at(r,'longitude'),alt,instant(at(r,'timestamp'),true),number(at(r,'speed'),0,2000,'speed',true),number(at(r,'course'),0,360,'course',true)));
    }
    altitudeReference='Recorded GPS altitude, WGS84 (feet)';
  }else{
    const doc=xml(raw);
    if(doc.gpx){
      format='GPX track';altitudeReference='Recorded GPX elevation (metres converted to feet; datum not independently verified)';
      let segment=0;
      for(const trk of list(doc.gpx.trk))for(const seg of list(trk.trkseg)){
        for(const p of list(seg.trkpt)){
          const elevation=number(p.ele,-1000,30000,'GPX elevation',true);
          push(point(p['@lat'],p['@lon'],elevation==null?null:elevation/0.3048,instant(text(p.time)),null,null,segment));
        }segment++;
      }
    }else if(doc.kml){
      format='KML track';const tracks=nodes(doc.kml,'Track');let segment=0;
      if(tracks.length)for(const trk of tracks){
        const coords=list(trk.coord),times=list(trk.when);
        if(times.length && times.length!==coords.length)bad('KML has unmatched coordinates and timestamps.');
        const absolute=text(trk.altitudeMode)==='absolute';
        if(!absolute)warnings.push('Non-absolute KML altitude omitted; no terrain-relative altitude is inferred.');
        for(let i=0;i<coords.length;i++){
          const xyz=text(coords[i]).split(/\s+/);if(xyz.length!==3)bad('Invalid KML gx:coord.');
          push(point(xyz[1],xyz[0],absolute?number(xyz[2],-1000,30000,'KML altitude')/0.3048:null,instant(text(times[i])),null,null,segment));
        }segment++;
      }else for(const line of nodes(doc.kml,'LineString')){
        const absolute=text(line.altitudeMode)==='absolute';
        for(const token of text(line.coordinates).split(/\s+/).filter(Boolean)){
          const xyz=token.split(',');if(xyz.length<2||xyz.length>3)bad('Invalid KML coordinate tuple.');
          push(point(xyz[1],xyz[0],absolute&&xyz[2]?number(xyz[2],-1000,30000,'KML altitude')/0.3048:null,null,null,null,segment));
        }segment++;
      }
      altitudeReference='KML absolute altitude (metres converted to feet); not a pressure altitude';
    }else bad('Upload a GPX track or KML track, not a route or unrelated XML document.');
  }
  if(points.length<2)bad('At least two recorded positions are required.');
  let distance=0,timing=true,gaps=0;
  for(let i=0;i<points.length;i++){
    if(!points[i].time)timing=false;
    if(!i)continue;const a=points[i-1],b=points[i];
    const dt=Date.parse(b.time)-Date.parse(a.time);
    if(a.segment===b.segment&&!(dt>120000))distance+=distanceNm(a,b);
    if(!(dt>0))timing=false;if(a.segment!==b.segment || dt>120000)gaps++;
  }
  if(!timing)warnings.push('Missing or non-increasing timestamps: animated replay and duration are disabled.');
  if(gaps)warnings.push(gaps+' segment break(s) or gaps over 120 seconds. Replay will pause across gaps.');
  const duration=timing?(Date.parse(points.at(-1).time)-Date.parse(points[0].time))/1000:null;
  return {kind:'track',format,points,warnings:[...new Set(warnings)],altitudeReference,
    summary:{pointCount:points.length,start:points[0],end:points.at(-1),durationSeconds:duration,distanceNm:Number(distance.toFixed(2)),replayAvailable:timing,gaps}};
}
const airport=v=>/^[A-Z]{4}$/.test(v||'')?v:'';
function parsePlan(raw,name){
  let format='Route text',fields={},waypoints=[],warnings=[];
  if(raw.trim().startsWith('<')){
    const doc=xml(raw);
    if(doc['flight-plan']){
      format='Garmin FPL';const root=doc['flight-plan'],routes=list(root.route);
      if(routes.length!==1)bad('FPL must contain exactly one route.');
      const table=list(root['waypoint-table']?.waypoint);
      for(const p of list(routes[0]['route-point'])){
        const matches=table.filter(w=>text(w.identifier)===text(p['waypoint-identifier']) && text(w.type)===text(p['waypoint-type']) && text(w['country-code'])===text(p['waypoint-country-code']));
        if(matches.length!==1)bad('FPL waypoint reference is missing or ambiguous.');
        const w=matches[0];waypoints.push({ident:text(w.identifier).toUpperCase(),lat:number(w.lat,-90,90,'waypoint latitude'),lon:number(w.lon,-180,180,'waypoint longitude'),type:text(w.type)});
      }
    }else if(doc.gpx){
      format='GPX route';const routes=list(doc.gpx.rte);if(routes.length!==1 || doc.gpx.trk)bad('Choose a GPX route file; recorded tracks belong in Import Track Log.');
      waypoints=list(routes[0].rtept).map(p=>({ident:text(p.name).toUpperCase(),lat:number(p['@lat'],-90,90,'latitude'),lon:number(p['@lon'],-180,180,'longitude'),type:'WAYPOINT'}));
      warnings.push('GPX route compatibility is not a claim that ForeFlight exports GPX flight plans.');
    }else bad('Supported route XML: Garmin flight-plan or GPX rte.');
    if(waypoints.length<2 || waypoints.length>80)bad('Route files need 2–80 waypoints.');
    fields={departure:airport(waypoints[0].ident),destination:airport(waypoints.at(-1).ident),
      route:waypoints.map(p=>p.ident || p.lat.toFixed(5)+','+p.lon.toFixed(5)).join(' ')};
    if(waypoints.some(w=>!w.ident))warnings.push('Unnamed coordinates retained in the source. Verify waypoint identifiers before planning.');
  }else if(/^\s*\(?FPL-/i.test(raw)){
    format='ICAO FPL text';const parts=raw.toUpperCase().trim().replace(/^\(/,'').replace(/\)$/,'').split(/\s*-\s*/);
    if(parts.length<8 || !/^[A-Z]{4}\d{4}$/.test(parts[5]))bad('ICAO FPL items 7–18 could not be identified unambiguously.');
    const dest=parts[7].match(/^([A-Z]{4})(\d{2})(\d{2})(?:\s+([A-Z]{4}))?/);if(!dest)bad('Invalid ICAO destination / EET.');
    const route=parts[6].match(/^([NKM]\d{3,4})([AF]\d{3}|[SM]\d{4}|VFR)\s+(.+)$/);
    if(!route)bad('ICAO speed, level and route could not be separated.');
    fields={callsign:parts[1],aircraftCode:parts[3].split('/')[0].replace(/^\d+/,''),departure:parts[5].slice(0,4),destination:dest[1],route:route[3],alternate:dest[4] || '',cruiseFlightLevel:route[2]};
    const registration=parts.slice(8).join('-').match(/\bREG\/([A-Z0-9-]+)/);if(registration)fields.aircraftRegistration=registration[1];
    else warnings.push('ICAO item 7 is an aircraft identification/callsign, not necessarily a registration. Confirm registration.');
    if(parts[2]?.startsWith('I'))fields.flightRules='IFR';else if(parts[2]?.startsWith('V'))fields.flightRules='VFR';else warnings.push('Mixed Y/Z flight rules need pilot resolution; not converted to IFR or VFR.');
    if(Number(dest[3])>59)bad('Invalid ICAO EET minutes.');fields.estimatedEnrouteMinutes=Number(dest[2])*60+Number(dest[3]);
    const dof=parts.slice(8).join('-').match(/\bDOF\/(\d{6})\b/);
    if(dof)fields.plannedDepartureUtc=instant('20'+dof[1].slice(0,2)+'-'+dof[1].slice(2,4)+'-'+dof[1].slice(4,6)+'T'+parts[5].slice(4,6)+':'+parts[5].slice(6)+':00Z');
    else warnings.push('ICAO departure time has no date of flight; enter a full UTC date/time.');
  }else{
    const labels={departure:['DEP','DEPARTURE','FROM'],destination:['DEST','DESTINATION','ARR','ARRIVAL','TO'],alternate:['ALTN','ALTERNATE'],route:['ROUTE'],cruiseFlightLevel:['LEVEL','ALTITUDE'],flightRules:['RULES','FLIGHT RULES'],aircraftRegistration:['REG','REGISTRATION','AIRCRAFT'],aircraftCode:['TYPE']};
    for(const [key,names] of Object.entries(labels)){const m=raw.match(new RegExp('^(?:'+names.join('|')+')\\s*[:=]\\s*([^\\r\\n]+)$','im'));if(m)fields[key]=m[1].trim().toUpperCase();}
    if(!fields.route){if(raw.includes('\n')&&Object.keys(fields).length===0)bad('Paste only the route, or use labeled Departure, Destination and Route lines.');fields.route=Object.keys(fields).length?'':raw.trim().toUpperCase();}
    const tokens=fields.route.split(/\s+/);
    if(!fields.departure)fields.departure=airport(tokens[0]);if(!fields.destination)fields.destination=airport(tokens.at(-1));
  }
  if(fields.route?.length>240)bad('Route exceeds the existing OFP 240-character limit. Shorten it explicitly; it will not be truncated.');
  if(fields.route?.trim().split(/\s+/).length>80)bad('Route exceeds 80 elements. Shorten it explicitly; it will not be truncated.');
  if(fields.route && !/^[A-Z0-9 ./,+-]+$/.test(fields.route))bad('Route contains unsupported characters.');
  for(const k of ['departure','destination','alternate'])if(fields[k]&&!airport(fields[k]))bad(k+' must be an unambiguous four-letter ICAO identifier.');
  if(fields.flightRules&&!['IFR','VFR'].includes(fields.flightRules))bad('Flight rules must be IFR or VFR.');
  if(fields.cruiseFlightLevel&&!/^(?:[AF]\d{3}|[SM]\d{4}|VFR|\d{3,5}(?:\s?FT)?)$/.test(fields.cruiseFlightLevel))bad('Unrecognized cruise level. Use Fxxx, Axxx or an explicit altitude.');
  return {kind:'plan',format,fields,waypoints,warnings,
    missing:['departure','destination','flightRules','aircraftRegistration','plannedDepartureUtc'].filter(k=>!fields[k])};
}
export function parseAviationImport(buffer,{kind,name='route.txt'}={}){
  if(!Buffer.isBuffer(buffer))buffer=Buffer.from(buffer);
  if(!buffer.length || buffer.length>MAX_IMPORT_BYTES)bad('Import files must be non-empty and no larger than 8 MiB.');
  const raw=buffer.toString('utf8').replace(/^\uFEFF/,'');if(raw.includes('\0')||raw.includes('\uFFFD'))bad('Use an uncompressed UTF-8 file. KMZ and binary files are not supported.');
  const parsed=kind==='track'?parseTrack(raw,name):kind==='plan'?parsePlan(raw,name):bad('Choose flight plan or track import.');
  return {...parsed,sha256:createHash('sha256').update(buffer).digest('hex'),filename:String(name).split(/[\\/]/).at(-1).slice(0,120)};
}
