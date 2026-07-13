import * as THREE from '/three.module.min.js';

const container = document.getElementById('operatingGlobe');
const loading = document.getElementById('globeLoading');
const fallback = document.getElementById('globeFallback');
const airportInfo = document.getElementById('globeAirportInfo');
const airportRail = document.getElementById('globeAirportRail');
const airportCount = document.getElementById('globeAirportCount');

const AIRPORTS = [
  { icao: 'EFHK', code: 'HEL', name: 'Helsinki-Vantaa', country: 'Finland', lat: 60.3172, lon: 24.9633 },
  { icao: 'EFHV', code: 'HYV', name: 'Hyvinkää', country: 'Finland', lat: 60.6544, lon: 24.8811 },
  { icao: 'EFNU', code: 'NUM', name: 'Nummela', country: 'Finland', lat: 60.3339, lon: 24.2964 },
  { icao: 'EFPR', code: 'PYT', name: 'Pyhtää', country: 'Finland', lat: 60.4917, lon: 26.7094 },
  { icao: 'EFHN', code: 'HNK', name: 'Hanko', country: 'Finland', lat: 59.8489, lon: 23.0836 },
  { icao: 'EFRY', code: 'RAY', name: 'Räyskälä', country: 'Finland', lat: 60.7447, lon: 24.1078 },
  { icao: 'EFLA', code: 'LAH', name: 'Lahti', country: 'Finland', lat: 61.1442, lon: 25.6935 },
  { icao: 'EFTU', code: 'TKU', name: 'Turku', country: 'Finland', lat: 60.5141, lon: 22.2628 },
  { icao: 'EFTP', code: 'TMP', name: 'Tampere', country: 'Finland', lat: 61.4141, lon: 23.6044 },
  { icao: 'EETN', code: 'TLL', name: 'Tallinn', country: 'Estonia', lat: 59.4133, lon: 24.8328 },
  { icao: 'EEKE', code: 'URE', name: 'Kuressaare', country: 'Estonia', lat: 58.2299, lon: 22.5095 },
  { icao: 'ESSB', code: 'BMA', name: 'Stockholm Bromma', country: 'Sweden', lat: 59.3544, lon: 17.9417 }
];

if (airportCount) airportCount.textContent = String(AIRPORTS.length);

function showFallback(message) {
  if (loading) loading.hidden = true;
  if (fallback) {
    fallback.textContent = message || fallback.textContent;
    fallback.hidden = false;
  }
}

function airportByCode(value) {
  const normalized = String(value || '').trim().toUpperCase();
  return AIRPORTS.find((airport) => airport.icao === normalized || airport.code === normalized);
}

function updateAirportInfo(airport, label = 'Network airport') {
  if (!airportInfo || !airport) return;
  airportInfo.innerHTML = `<span>${label}</span><strong>${airport.name}</strong><small>${airport.code} / ${airport.country}</small>`;
  airportRail?.querySelectorAll('.globe-airport-button').forEach((button) => {
    button.classList.toggle('is-active', button.dataset.airport === airport.icao);
  });
}

if (!container) {
  showFallback('The 360° network view could not be loaded.');
} else {
  try {
    const testCanvas = document.createElement('canvas');
    if (!testCanvas.getContext('webgl2')) throw new Error('WebGL 2 is unavailable');

    const renderer = new THREE.WebGLRenderer({
      antialias: true,
      alpha: true,
      powerPreference: 'high-performance',
      preserveDrawingBuffer: true
    });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 1.8));
    renderer.setClearColor(0x000000, 0);
    renderer.outputColorSpace = THREE.SRGBColorSpace;
    renderer.toneMapping = THREE.ACESFilmicToneMapping;
    renderer.toneMappingExposure = 0.82;
    renderer.domElement.setAttribute('aria-hidden', 'true');
    container.appendChild(renderer.domElement);

    const scene = new THREE.Scene();
    const camera = new THREE.PerspectiveCamera(34, 1, 0.1, 100);
    let cameraDistance = 4.7;
    let targetCameraDistance = cameraDistance;
    camera.position.set(0, 0, cameraDistance);

    const pitchGroup = new THREE.Group();
    const yawGroup = new THREE.Group();
    pitchGroup.add(yawGroup);
    scene.add(pitchGroup);

    scene.add(new THREE.HemisphereLight(0xbfd2ee, 0x020817, 1.5));
    const keyLight = new THREE.DirectionalLight(0xffffff, 2.15);
    keyLight.position.set(-3, 2.8, 4.2);
    scene.add(keyLight);
    const rimLight = new THREE.DirectionalLight(0x6f9ee8, 1.2);
    rimLight.position.set(4, -1.5, -2);
    scene.add(rimLight);

    const mobile = window.matchMedia('(max-width: 700px)').matches;
    const globeRadius = 1.38;
    const globeGeometry = new THREE.SphereGeometry(globeRadius, mobile ? 64 : 96, mobile ? 40 : 64);
    const globeMaterial = new THREE.MeshStandardMaterial({
      color: 0x7b8ba3,
      roughness: 0.96,
      metalness: 0.02
    });
    const globe = new THREE.Mesh(globeGeometry, globeMaterial);
    yawGroup.add(globe);

    const textureLoader = new THREE.TextureLoader();
    textureLoader.load(
      '/earth-atmos-2048.jpg',
      (texture) => {
        texture.colorSpace = THREE.SRGBColorSpace;
        texture.anisotropy = Math.min(8, renderer.capabilities.getMaxAnisotropy());
        globeMaterial.map = texture;
        globeMaterial.needsUpdate = true;
        if (loading) loading.hidden = true;
        window.__NGA_GLOBE_READY__ = true;
      },
      undefined,
      () => {
        if (loading) loading.hidden = true;
        window.__NGA_GLOBE_READY__ = true;
      }
    );

    const grid = new THREE.Mesh(
      new THREE.SphereGeometry(globeRadius + 0.014, 36, 24),
      new THREE.MeshBasicMaterial({ color: 0xb9cae3, wireframe: true, transparent: true, opacity: 0.035 })
    );
    yawGroup.add(grid);

    const atmosphere = new THREE.Mesh(
      new THREE.SphereGeometry(globeRadius + 0.075, 64, 40),
      new THREE.MeshBasicMaterial({
        color: 0x8bb7ff,
        transparent: true,
        opacity: 0.085,
        side: THREE.BackSide,
        blending: THREE.AdditiveBlending
      })
    );
    pitchGroup.add(atmosphere);

    const haloTexture = (() => {
      const canvas = document.createElement('canvas');
      canvas.width = 96;
      canvas.height = 96;
      const context = canvas.getContext('2d');
      const gradient = context.createRadialGradient(48, 48, 3, 48, 48, 45);
      gradient.addColorStop(0, 'rgba(255,255,255,1)');
      gradient.addColorStop(0.16, 'rgba(255,255,255,0.72)');
      gradient.addColorStop(0.45, 'rgba(255,255,255,0.2)');
      gradient.addColorStop(1, 'rgba(255,255,255,0)');
      context.fillStyle = gradient;
      context.fillRect(0, 0, 96, 96);
      const texture = new THREE.CanvasTexture(canvas);
      texture.colorSpace = THREE.SRGBColorSpace;
      return texture;
    })();

    function latLonPosition(lat, lon, radius) {
      const phi = THREE.MathUtils.degToRad(90 - lat);
      const theta = THREE.MathUtils.degToRad(lon + 180);
      return new THREE.Vector3(
        -radius * Math.sin(phi) * Math.cos(theta),
        radius * Math.cos(phi),
        radius * Math.sin(phi) * Math.sin(theta)
      );
    }

    const markers = AIRPORTS.map((airport) => {
      const position = latLonPosition(airport.lat, airport.lon, globeRadius + 0.035);
      const material = new THREE.MeshBasicMaterial({ color: 0xd9e1ec, toneMapped: false });
      const mesh = new THREE.Mesh(new THREE.SphereGeometry(0.027, 18, 14), material);
      mesh.position.copy(position);
      mesh.userData.airport = airport;
      yawGroup.add(mesh);

      const haloMaterial = new THREE.SpriteMaterial({
        map: haloTexture,
        color: 0xd9e1ec,
        transparent: true,
        opacity: 0.55,
        depthTest: true,
        depthWrite: false,
        blending: THREE.AdditiveBlending,
        toneMapped: false
      });
      const halo = new THREE.Sprite(haloMaterial);
      halo.position.copy(position.clone().multiplyScalar(1.006));
      halo.scale.setScalar(0.14);
      halo.userData.airport = airport;
      yawGroup.add(halo);
      return { airport, mesh, halo };
    });

    if (airportRail) {
      const fragment = document.createDocumentFragment();
      AIRPORTS.forEach((airport) => {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'globe-airport-button';
        button.dataset.airport = airport.icao;
        button.textContent = `${airport.code} / ${airport.name}`;
        button.addEventListener('click', () => focusAirport(airport, 'Network airport'));
        fragment.appendChild(button);
      });
      airportRail.appendChild(fragment);
    }

    let currentPitch = THREE.MathUtils.degToRad(60.6544);
    let currentYaw = THREE.MathUtils.degToRad(-90 - 24.8811);
    let targetPitch = currentPitch;
    let targetYaw = currentYaw;
    let focusMoving = false;
    let lastInteraction = performance.now();
    const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

    pitchGroup.rotation.x = currentPitch;
    yawGroup.rotation.y = currentYaw;

    function focusAirport(airport, label) {
      if (!airport) return;
      targetPitch = THREE.MathUtils.degToRad(airport.lat);
      targetYaw = THREE.MathUtils.degToRad(-90 - airport.lon);
      focusMoving = true;
      lastInteraction = performance.now();
      updateAirportInfo(airport, label);
    }

    function routeValue(select) {
      if (!select) return '';
      return select.value || select.options[select.selectedIndex]?.value || '';
    }

    function syncRouteMarkers(focusTarget = '') {
      const departure = airportByCode(routeValue(document.getElementById('dep')));
      const destination = airportByCode(routeValue(document.getElementById('arr')));

      markers.forEach(({ airport, mesh, halo }) => {
        const isDeparture = departure?.icao === airport.icao;
        const isDestination = destination?.icao === airport.icao;
        const color = isDestination ? 0x36c77b : (isDeparture ? 0xff7a2f : 0xd9e1ec);
        mesh.material.color.setHex(color);
        halo.material.color.setHex(color);
        mesh.scale.setScalar(isDeparture || isDestination ? 1.45 : 1);
        halo.scale.setScalar(isDeparture || isDestination ? 0.21 : 0.14);
        halo.material.opacity = isDeparture || isDestination ? 0.82 : 0.48;
      });

      if (focusTarget === 'departure' && departure) focusAirport(departure, 'Selected departure');
      if (focusTarget === 'destination' && destination) focusAirport(destination, 'Selected destination');
    }

    const departureSelect = document.getElementById('dep');
    const destinationSelect = document.getElementById('arr');
    departureSelect?.addEventListener('change', () => syncRouteMarkers('departure'));
    destinationSelect?.addEventListener('change', () => syncRouteMarkers('destination'));
    syncRouteMarkers();
    updateAirportInfo(airportByCode('EFHV'));

    const pointers = new Map();
    let isDragging = false;
    let dragDistance = 0;
    let pinchDistance = 0;
    let lastPointer = { x: 0, y: 0 };

    function pointerDistance() {
      const points = [...pointers.values()];
      if (points.length < 2) return 0;
      return Math.hypot(points[0].x - points[1].x, points[0].y - points[1].y);
    }

    container.addEventListener('pointerdown', (event) => {
      container.setPointerCapture?.(event.pointerId);
      pointers.set(event.pointerId, { x: event.clientX, y: event.clientY });
      lastPointer = { x: event.clientX, y: event.clientY };
      pinchDistance = pointerDistance();
      dragDistance = 0;
      isDragging = true;
      focusMoving = false;
      lastInteraction = performance.now();
      container.classList.add('is-dragging');
    });

    container.addEventListener('pointermove', (event) => {
      if (!pointers.has(event.pointerId)) return;
      const previous = pointers.get(event.pointerId);
      pointers.set(event.pointerId, { x: event.clientX, y: event.clientY });
      lastInteraction = performance.now();

      if (pointers.size > 1) {
        const nextPinchDistance = pointerDistance();
        if (pinchDistance) {
          targetCameraDistance = THREE.MathUtils.clamp(
            targetCameraDistance - (nextPinchDistance - pinchDistance) * 0.012,
            3.65,
            6.2
          );
        }
        pinchDistance = nextPinchDistance;
        return;
      }

      const dx = event.clientX - previous.x;
      const dy = event.clientY - previous.y;
      currentYaw += dx * 0.0052;
      currentPitch = THREE.MathUtils.clamp(currentPitch + dy * 0.0042, -1.28, 1.28);
      targetYaw = currentYaw;
      targetPitch = currentPitch;
      dragDistance += Math.abs(dx) + Math.abs(dy);
      lastPointer = { x: event.clientX, y: event.clientY };
    });

    function selectMarkerAt(event) {
      const bounds = renderer.domElement.getBoundingClientRect();
      const pointer = new THREE.Vector2(
        ((event.clientX - bounds.left) / bounds.width) * 2 - 1,
        -((event.clientY - bounds.top) / bounds.height) * 2 + 1
      );
      const raycaster = new THREE.Raycaster();
      raycaster.setFromCamera(pointer, camera);
      const intersections = raycaster.intersectObjects([globe, ...markers.map(({ mesh }) => mesh)], false);
      const hit = intersections.find((intersection) => intersection.object.userData.airport);
      const globeHit = intersections.find((intersection) => intersection.object === globe);
      if (hit && (!globeHit || hit.distance < globeHit.distance)) {
        focusAirport(hit.object.userData.airport, 'Network airport');
      }
    }

    function endPointer(event) {
      if (!pointers.has(event.pointerId)) return;
      if (pointers.size === 1 && dragDistance < 8) selectMarkerAt(event);
      pointers.delete(event.pointerId);
      pinchDistance = pointerDistance();
      if (!pointers.size) {
        isDragging = false;
        container.classList.remove('is-dragging');
      }
      lastInteraction = performance.now();
    }

    container.addEventListener('pointerup', endPointer);
    container.addEventListener('pointercancel', endPointer);
    container.addEventListener('wheel', (event) => {
      event.preventDefault();
      targetCameraDistance = THREE.MathUtils.clamp(targetCameraDistance + event.deltaY * 0.0024, 3.65, 6.2);
      lastInteraction = performance.now();
    }, { passive: false });

    let visible = true;
    const visibilityObserver = new IntersectionObserver((entries) => {
      visible = entries[0]?.isIntersecting ?? true;
    }, { rootMargin: '160px' });
    visibilityObserver.observe(container);

    const resizeObserver = new ResizeObserver(() => {
      const width = Math.max(1, container.clientWidth);
      const height = Math.max(1, container.clientHeight);
      renderer.setSize(width, height, false);
      camera.aspect = width / height;
      camera.updateProjectionMatrix();
    });
    resizeObserver.observe(container);

    function animate(time) {
      requestAnimationFrame(animate);
      if (!visible) return;

      if (focusMoving) {
        const yawDelta = Math.atan2(Math.sin(targetYaw - currentYaw), Math.cos(targetYaw - currentYaw));
        currentYaw += yawDelta * 0.075;
        currentPitch += (targetPitch - currentPitch) * 0.075;
        if (Math.abs(yawDelta) < 0.001 && Math.abs(targetPitch - currentPitch) < 0.001) focusMoving = false;
      } else if (!isDragging && !prefersReducedMotion && time - lastInteraction > 4200) {
        currentYaw += 0.00055;
        targetYaw = currentYaw;
      }

      cameraDistance += (targetCameraDistance - cameraDistance) * 0.09;
      camera.position.z = cameraDistance;
      pitchGroup.rotation.x = currentPitch;
      yawGroup.rotation.y = currentYaw;
      renderer.render(scene, camera);
    }

    requestAnimationFrame(animate);
    window.__NGA_GLOBE_RENDERER__ = renderer;
  } catch (error) {
    showFallback('The 360° view is unavailable on this device. The airport selectors above remain fully available.');
  }
}
