// Local-only simulator fixture. Use the production JS adapter with disclosed
// training records, without reading credentials or authenticating a pilot.
import http from 'node:http';
import {readFileSync} from 'node:fs';
const adapter = readFileSync(new URL('../../public/pilot-flight-deck.js',import.meta.url));
const fixture = `<!doctype html><html lang="en" data-efb-theme="night"><meta name="viewport" content="width=device-width,initial-scale=1">
<style>html{font:18px system-ui;background:#10253d;color:white;padding:32px}button{font:inherit;padding:14px;border-radius:12px}</style>
<main><h1>Flight operations</h1><p>Simulator fixture · two training flights</p><button id="flightSwitchBtn">Select flight</button>
<p id="result" aria-live="polite">No flight loaded</p></main>
<dialog id="flightPickerDialog"><button id="flightPickerCloseBtn">Done</button><input id="flightPickerSearch"><p id="flightPickerCount"></p>
<div id="flightPickerResults"></div><button id="flightPickerCreateBtn">Create</button></dialog>
<dialog id="flightChangeDialog"><p id="flightChangeMessage"></p><button id="flightChangeSaveBtn">Save</button><button id="flightChangeCancelBtn">Cancel</button></dialog>
<span id="appDraftBadge" hidden></span><script src="/pilot-flight-deck.js"></script>
<script>
let selected=null; const flights=[{id:'DEMO-01',dep:'EFHV',arr:'EFHN',requestDate:'2026-11-15',requestTime:'10:00',aircraft:'PA-28',manualFlight:true},
{id:'DEMO-02',dep:'EFHK',arr:'EFTP',requestDate:'2026-11-16',requestTime:'14:15',aircraft:'C172',status:'Scheduled'}];
createPilotFlightDeck({getFlights:()=>flights,getSelected:()=>selected,getPlanner:()=>null,today:()=>'2026-11-15',
choose:f=>{selected=f;document.querySelector('#result').textContent='Active: '+f.id},create:()=>{document.querySelector('#result').textContent='Create requested'},
escapeHtml:s=>String(s)});
</script></html>`;
http.createServer((req,res)=>{
  if(req.url==='/booking-ops') res.writeHead(200,{'content-type':'text/html; charset=utf-8'}).end(fixture);
  else if(req.url==='/pilot-flight-deck.js') res.writeHead(200,{'content-type':'application/javascript'}).end(adapter);
  else res.writeHead(404).end();
}).listen(4179,'127.0.0.1');
