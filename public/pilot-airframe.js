/* Original representative single-engine airframe, metres; not a type model. */
import * as T from '/vendor/three/three.module.js';

function buildAirframe() {
  const group=new T.Group();
  const mat=(color,metalness=.25,roughness=.38)=>new T.MeshStandardMaterial({color,metalness,roughness,side:T.DoubleSide});
  const enamel=mat(0xe1e5ea), navy=mat(0x101f3b), glass=mat(0x344b61,.45,.19), alloy=mat(0xa0a5ac,.55), rubber=mat(0x232832,.03,.85);
  function mesh(geometry,material,parent=group){
    const m=new T.Mesh(geometry,material);m.castShadow=true;m.receiveShadow=true;parent.add(m);return m;
  }
  // Rings run tail to spinner. Each station defines width, height and centre.
  function hull(stations,material,stripe=false) {
    const vertices=[],indices=[], geo=new T.BufferGeometry(),n=48;
    for(const [x,w,h,y] of stations)for(let i=0;i<n;i++){
      const a=i*2*Math.PI/n;vertices.push(x,y+h*Math.sin(a),w*Math.cos(a));
    }
    for(let r=1;r<stations.length;r++)for(let i=0;i<n;i++){
      const a=(r-1)*n+i,b=(r-1)*n+(i+1)%n,c=a+n,d=b+n;
      if(stripe)geo.addGroup(indices.length,6,[0,24,25,47].includes(i)?1:0);
      indices.push(a,c,b,b,c,d);
    }
    geo.setAttribute('position',new T.Float32BufferAttribute(vertices,3));geo.setIndex(indices);geo.computeVertexNormals();
    if(stripe){ // Batch faces by material rather than issuing one draw per quad.
      const reordered=[],counts=[0,0];for(const index of [0,1])for(const g of geo.groups)if(g.materialIndex===index){reordered.push(...indices.slice(g.start,g.start+g.count));counts[index]+=g.count;}
      geo.clearGroups();geo.setIndex(reordered);geo.addGroup(0,counts[0],0);geo.addGroup(counts[0],counts[1],1);
    }
    return mesh(geo,material);
  }
  hull([[-3.85,.02,.04,.40],[-3.52,.16,.21,.38],[-2.5,.30,.35,.29],[-1.25,.47,.46,.18],[-.65,.61,.52,.13],[.28,.67,.55,.12],[1.16,.63,.49,.11],[1.75,.54,.43,.09],[2.37,.49,.41,.11],[2.69,.40,.34,.11],[2.74,.02,.03,.11]],[enamel,navy],true);
  hull([[-1.28,.03,.03,.57],[-.91,.39,.18,.60],[-.43,.55,.43,.63],[.12,.58,.48,.63],[.71,.55,.43,.61],[1.38,.20,.07,.56],[1.44,.02,.02,.55]],glass);
  hull([[-.53,.49,.38,.66],[-.47,.55,.43,.65],[-.41,.55,.43,.65]],alloy);
  hull([[.38,.57,.47,.625],[.43,.58,.47,.625],[.48,.57,.46,.625]],alloy);
  function foil(side, stations,material,vertical=false){
    const verts=[],idx=[],n=40;
    for(const [span,leading,chord,y] of stations)for(let i=0;i<n;i++){
      const angle=i*2*Math.PI/n,u=(1-Math.cos(angle))*.5;
      const h=chord*(.065*Math.sin(angle)*Math.sqrt(1-u+.015)+.014*Math.sin(Math.PI*u));
      verts.push(leading-chord*u,vertical?span:y+h,vertical?h:side*span);
    }
    for(let k=1;k<stations.length;k++)for(let i=0;i<n;i++){const a=(k-1)*n+i,b=(k-1)*n+(i+1)%n;idx.push(a,b,a+n,b,b+n,a+n);}
    const geo=new T.BufferGeometry();geo.setAttribute('position',new T.Float32BufferAttribute(verts,3));geo.setIndex(idx);geo.computeVertexNormals();mesh(geo,material);
  }
  function ellipsoid(x,y,z,sx,sy,sz,material){const m=mesh(new T.SphereGeometry(1,24,16),material);m.scale.set(sx,sy,sz);m.position.set(x,y,z);return m;}
  function rod(a,b,r,material){const from=new T.Vector3(...a),to=new T.Vector3(...b),diff=to.clone().sub(from);const m=mesh(new T.CylinderGeometry(r,r,diff.length(),12),material);m.position.copy(from).lerp(to,.5);m.quaternion.setFromUnitVectors(new T.Vector3(0,1,0),diff.normalize());}
  for(const side of [-1,1]){
    foil(side,[[.2,.86,1.88,-.20],[.88,.83,1.84,-.16],[3.4,.48,1.53,.02],[4.65,.28,1.23,.14],[4.93,.04,.83,.18],[5.0,-.29,.17,.19]],enamel);
    foil(side,[[.06,-2.77,.99,.32],[1.12,-2.93,.84,.38],[1.79,-3.14,.58,.44],[1.85,-3.38,.08,.45]],enamel);
    const lampMat=mat(side===1?0x39704e:0x943d3c,.12);ellipsoid(-.24,.20,4.96*side,.13,.035,.04,lampMat);
  }
  foil(1,[[.40,-2.32,1.49,0],[.74,-2.70,1.20,0],[1.53,-3.28,.61,0],[1.73,-3.52,.32,0],[1.75,-3.63,.10,0]],navy,true);
  for(const [x,z] of [[1.84,0],[-.37,1.22],[-.37,-1.22]]){
    rod([x+.10,-.10,z],[x,-.85,z],.045,alloy);
    const wheel=mesh(new T.CylinderGeometry(.224,.224,.13,28),rubber);wheel.rotation.x=Math.PI/2;wheel.position.set(x,-.89,z);
    const hub=mesh(new T.CylinderGeometry(.107,.107,.145,20),alloy);hub.rotation.x=Math.PI/2;hub.position.copy(wheel.position);
  }
  for(const sign of [-1,1]){const blade=ellipsoid(2.84,.11+sign*.50,sign*.26,.045,.55,.098,rubber);blade.rotation.x=.46;}
  hull([[2.73,.25,.25,.11],[2.9,.21,.21,.11],[3.07,.09,.09,.11],[3.10,.006,.006,.11]],alloy);
  return group;
}

export function mountAirframe(host,onLost){
  const renderer=new T.WebGLRenderer({antialias:true,alpha:false,powerPreference:'low-power'});
  const scene=new T.Scene(), camera=new T.PerspectiveCamera(34,1,.1,100), target=new T.Vector3(0,.15,0);
  scene.background=new T.Color(0x182538);renderer.setPixelRatio(Math.min(devicePixelRatio || 1,1.5));
  renderer.shadowMap.enabled=true;renderer.shadowMap.type=T.PCFSoftShadowMap;
  renderer.toneMapping=T.ACESFilmicToneMapping;renderer.toneMappingExposure=1.28;
  scene.add(new T.HemisphereLight(0xeaf3ff,0x657183,2));
  const key=new T.DirectionalLight(0xfff7e9,3.1);key.position.set(4,8,5);key.castShadow=true;
  Object.assign(key.shadow.camera,{left:-7,right:7,top:7,bottom:-7});key.shadow.mapSize.set(1024,1024);key.shadow.normalBias=.025;scene.add(key);
  const rim=new T.DirectionalLight(0xb9d7ff,2);rim.position.set(-4,4,-5);scene.add(rim,buildAirframe());
  const ground=new T.Mesh(new T.PlaneGeometry(180,180),new T.MeshStandardMaterial({color:0x152338,roughness:.88}));ground.rotation.x=-Math.PI/2;ground.position.y=-1.12;ground.receiveShadow=true;scene.add(ground);
  host.replaceChildren(renderer.domElement);const canvas=renderer.domElement;
  let yaw=.78,pitch=.36,pointer=null,frame=0,disposed=false;
  function invalidate(){if(disposed || frame)return;frame=requestAnimationFrame(()=>{frame=0;const w=host.clientWidth,h=host.clientHeight;if(!w || !h)return;
    if(canvas.clientWidth!==w || canvas.clientHeight!==h || camera.aspect!==w/h){renderer.setSize(w,h,false);camera.aspect=w/h;camera.updateProjectionMatrix();}
    const distance=13.1*Math.max(1,.95/camera.aspect);camera.position.set(distance*Math.cos(yaw)*Math.cos(pitch),distance*Math.sin(pitch)+target.y,distance*Math.sin(yaw)*Math.cos(pitch));camera.lookAt(target);renderer.render(scene,camera);});}
  // The canvas uses percentage sizing; set the backing buffer before first paint and on resize.
  const resize=new ResizeObserver(()=>{if(!host.clientWidth || !host.clientHeight)return;renderer.setSize(host.clientWidth,host.clientHeight,false);camera.aspect=host.clientWidth/host.clientHeight;camera.updateProjectionMatrix();invalidate();});resize.observe(host);
  canvas.onpointerdown=e=>{if(pointer)return;canvas.setPointerCapture(e.pointerId);pointer=[e.pointerId,e.clientX,e.clientY];};
  canvas.onpointermove=e=>{if(pointer?.[0]!==e.pointerId)return;yaw-=(e.clientX-pointer[1])*.009;pitch=Math.max(.03,Math.min(1.35,pitch+(e.clientY-pointer[2])*.008));pointer=[e.pointerId,e.clientX,e.clientY];invalidate();};
  canvas.onpointerup=canvas.onlostpointercapture=()=>{pointer=null;};
  const lost=e=>{e.preventDefault();onLost();};canvas.addEventListener('webglcontextlost',lost);invalidate();
  return {turn(dir){if(!dir){yaw=.78;pitch=.36;}else yaw+=dir*Math.PI/8;invalidate();},dispose(){
    if(disposed)return;disposed=true;cancelAnimationFrame(frame);resize.disconnect();canvas.removeEventListener('webglcontextlost',lost);
    const materials=new Set();scene.traverse(item=>{item.geometry?.dispose();if(item.material)[item.material].flat().forEach(m=>materials.add(m));});
    materials.forEach(m=>m.dispose());key.shadow.dispose();renderer.dispose();renderer.forceContextLoss();canvas.remove();
  }};
}
