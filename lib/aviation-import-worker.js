import {parentPort,workerData} from 'node:worker_threads';
import {parseAviationImport} from './aviation-import.js';
try{parentPort.postMessage({result:parseAviationImport(Buffer.from(workerData.bytes),workerData.options)});}
catch(e){parentPort.postMessage({error:e.message});}
