/* A facade over the existing canonical records, not another flight store.
   Consumers receive one explicit planning view; recordings stay separate. */
window.createPilotFlightContext=function({getFlight,getFlights,activate,unload,upsert}){
  const listeners=new Set();
  function view(){
    const file=getFlight();if(!file)return null;
    const p=file.operationalFlightPlan || {};
    return Object.freeze({id:file.id,record:file,dep:p.departure || file.dep,arr:p.destination || file.arr,
      alternate:p.alternate || '',route:p.route || file.route,aircraft:p.aircraftRegistration || file.aircraft,
      source:file.importProvenance?.source || 'NGA',trackCount:file.trackCount || 0});
  }
  return Object.freeze({current:view,all:getFlights,activate,unload,upsert,
    subscribe(fn){listeners.add(fn);return ()=>listeners.delete(fn);},
    notify(){const state=view();for(const fn of listeners)try{fn(state);}catch(e){console.error('Flight view:',e);}}
  });
};
