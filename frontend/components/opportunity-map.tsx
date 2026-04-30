"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import mapboxgl, { GeoJSONSource } from "mapbox-gl";

import { RichTextBlock } from "@/components/rich-text-block";
import { SelectionInfoCard } from "@/components/selection-info-card";
import { formatDate, formatLabel, formatScore } from "@/lib/formatters";
import { branchLegendItems, colorForBranch, colorForTrigger, toneForBranch, toneForTrigger, triggerLegendItems } from "@/lib/semantics";
import { MapMarker, OpportunitySummary } from "@/types/api";
import { BranchKey, EntityRecord, InsightCardData } from "@/types/view";

type Props = {
  markers: MapMarker[];
  items: OpportunitySummary[];
  selectedClusterId: string;
  selectedCluster: OpportunitySummary;
  focusEntities: EntityRecord[];
  onSelectCluster: (clusterId: string) => void;
  onSelectBranch: (branch: BranchKey) => void;
};

type CoordinateMap = Record<string, [number, number]>;

type PositionedMarker = {
  item: OpportunitySummary;
  marker: MapMarker | null;
  baseCoordinates: [number, number];
  coordinates: [number, number];
};

const GEO_CACHE_KEY = "idc-map-geocodes-v1";

const COUNTRY_COORDINATES: Record<string, [number, number]> = {
  Austria: [14.55, 47.52],
  Belgium: [4.47, 50.5],
  Finland: [25.75, 61.92],
  France: [2.21, 46.23],
  Germany: [10.45, 51.17],
  Greece: [21.82, 39.07],
  Italy: [12.57, 41.87],
  Lithuania: [23.88, 55.17],
  Luxembourg: [6.13, 49.82],
  Netherlands: [5.29, 52.13],
  Spain: [-3.75, 40.46],
  Sweden: [18.64, 60.13],
  Switzerland: [8.23, 46.82],
  "United Kingdom": [-2.5, 54.8],
  "United States": [-98.58, 39.83],
};

const BRANCH_META: Record<BranchKey, { color: string; angle: number; radius: number }> = {
  direct: { color: colorForBranch("direct"), angle: -90, radius: 2.1 },
  peer: { color: colorForBranch("peer"), angle: 180, radius: 3.2 },
  ownership: { color: colorForBranch("ownership"), angle: 0, radius: 3.2 },
};

function buildAddress(marker?: MapMarker | null): string {
  if (!marker) return "";
  return [marker.address, marker.city, marker.state, marker.country].filter(Boolean).join(", ");
}

function locationPrecision(marker?: MapMarker | null): string {
  if (!marker) return "Region-level";
  if (marker.address) return "Address-level";
  if (marker.city || marker.state) return "City / state";
  if (marker.country) return "Country-level";
  return "Region-level";
}

function fallbackCoordinates(marker?: MapMarker | null): [number, number] | null {
  if (!marker) return null;
  if (typeof marker.longitude === "number" && typeof marker.latitude === "number") {
    return [marker.longitude, marker.latitude];
  }
  if (marker.country && COUNTRY_COORDINATES[marker.country]) {
    return COUNTRY_COORDINATES[marker.country];
  }
  return null;
}

function buildGeocodeQuery(marker: MapMarker): string | null {
  const parts = [marker.address, marker.city, marker.state, marker.country].filter(Boolean);
  if (!parts.length) return null;
  if (!marker.address && !marker.city && !marker.state) return null;
  return parts.join(", ");
}

function spreadCoordinates(base: [number, number], index: number, total: number, exactLocation: boolean): [number, number] {
  if (total <= 1) return base;

  const itemsPerRing = exactLocation ? 5 : 6;
  const ring = Math.floor(index / itemsPerRing);
  const slot = index % itemsPerRing;
  const itemsInRing = Math.min(total - ring * itemsPerRing, itemsPerRing);
  const angle = -Math.PI / 2 + (Math.PI * 2 * slot) / Math.max(itemsInRing, 1);
  const radius = (exactLocation ? 0.12 : 0.62) + ring * (exactLocation ? 0.08 : 0.22);
  return [base[0] + Math.cos(angle) * radius, base[1] + Math.sin(angle) * radius * 0.62];
}

function offsetPoint(center: [number, number], angle: number, radius: number): [number, number] {
  const radians = angle * (Math.PI / 180);
  return [center[0] + Math.cos(radians) * radius, center[1] + Math.sin(radians) * radius * 0.62];
}

function buildSelectedEventCard(cluster: OpportunitySummary, marker?: MapMarker | null): InsightCardData {
  const locationLabel = buildAddress(marker) || cluster.subject_country || cluster.subject_region || "Unknown location";
  return {
    eyebrow: "Selected Trigger",
    title: cluster.subject_company_name,
    subtitle: `${formatLabel(cluster.trigger_type)} · ${cluster.subject_country ?? "Unknown country"} · ${cluster.subject_region ?? "Unknown region"}`,
    badges: [
      { label: formatLabel(cluster.trigger_type), tone: toneForTrigger(cluster.trigger_type) },
      { label: cluster.subject_country ?? "Unknown country", tone: "neutral" },
      { label: locationPrecision(marker), tone: "purple" },
      { label: cluster.best_route_to_market ?? "Mixed", tone: "blue" },
    ],
    metrics: [
      { label: "Priority", value: String(formatScore(cluster.cluster_priority_score)), tone: "amber" },
      { label: "Confidence", value: String(formatScore(cluster.cluster_confidence_score)), tone: "green" },
      { label: "Opportunity", value: String(formatScore(cluster.opportunity_score)), tone: "blue" },
    ],
    sections: [
      { title: "Headline", text: cluster.event_headline ?? "No headline available." },
      { title: "Mapped Location", text: locationLabel },
      {
        title: "Event Summary",
        text: cluster.event_summary ?? "No event summary is available yet.",
      },
      {
        title: "Propagation Thesis",
        text: cluster.propagation_thesis ?? "No propagation thesis is available yet.",
      },
    ],
  };
}

function buildMarkerCard(item: OpportunitySummary, marker?: MapMarker | null): InsightCardData {
  const locationLabel = buildAddress(marker) || item.subject_country || item.subject_region || "Unknown location";
  return {
    eyebrow: "Map Event",
    title: item.subject_company_name,
    subtitle: `${formatLabel(item.trigger_type)} · ${item.subject_country ?? "Unknown country"} · ${formatDate(item.event_date)}`,
    badges: [
      { label: formatLabel(item.trigger_type), tone: toneForTrigger(item.trigger_type) },
      { label: item.subject_country ?? "Unknown country", tone: "neutral" },
      { label: locationPrecision(marker), tone: "purple" },
    ],
    metrics: [
      { label: "Priority", value: String(formatScore(item.cluster_priority_score)), tone: "amber" },
      { label: "Confidence", value: String(formatScore(item.cluster_confidence_score)), tone: "green" },
      { label: "Opportunity", value: String(formatScore(item.opportunity_score)), tone: "blue" },
    ],
    sections: [
      {
        title: "Mapped Location",
        text: locationLabel,
      },
      {
        title: "Event Summary",
        text: item.event_summary ?? "No summary is available for this event.",
      },
      {
        title: "What happens on select",
        text: "Selecting this event promotes it to the active trigger, recenters the map, and refreshes the cluster, graph, and event-intelligence views around it.",
      },
    ],
  };
}

function buildBranchCard(branch: BranchKey, entities: EntityRecord[], cluster: OpportunitySummary): InsightCardData {
  const topTitles = entities.flatMap((entity) => entity.recommendations[0]?.recommended_titles ?? []).slice(0, 8);
  return {
    eyebrow: "Opportunity Branch",
    title: `${formatLabel(branch)} opportunities`,
    subtitle: `${cluster.subject_company_name} · ${entities.length} mapped nodes`,
    badges: [
      { label: formatLabel(branch), tone: toneForBranch(branch) },
      { label: cluster.subject_country ?? "Unknown country", tone: "neutral" },
    ],
    metrics: [
      {
        label: "Avg priority",
        value: String(formatScore(entities.reduce((sum, entity) => sum + entity.priority_score, 0) / Math.max(entities.length, 1))),
        tone: "amber",
      },
      {
        label: "Avg confidence",
        value: String(formatScore(entities.reduce((sum, entity) => sum + entity.confidence_score, 0) / Math.max(entities.length, 1))),
        tone: "green",
      },
    ],
    sections: [
      {
        title: "Branch meaning",
        text:
          branch === "direct"
            ? "These nodes represent the immediate buyer path tied directly to the selected trigger event."
            : branch === "peer"
              ? "These peer candidates are commercially adjacent and may inherit similar scrutiny or response needs."
              : "These ownership-side candidates frame sponsor, governance, and portfolio-level opportunity paths around the selected trigger.",
      },
      {
        title: "Top entities",
        chips: entities.slice(0, 8).map((entity) => ({ label: entity.entity_name, tone: toneForBranch(branch) })),
      },
      {
        title: "Likely outreach",
        chips: topTitles.map((title) => ({ label: title, tone: "purple" })),
      },
    ],
  };
}

export function OpportunityMap({
  markers,
  items,
  selectedClusterId,
  selectedCluster,
  focusEntities,
  onSelectCluster,
  onSelectBranch,
}: Props) {
  const ref = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<mapboxgl.Map | null>(null);
  const selectedMarkerRef = useRef<mapboxgl.Marker | null>(null);
  const branchMarkersRef = useRef<mapboxgl.Marker[]>([]);
  const positionedMarkersRef = useRef<PositionedMarker[]>([]);
  const selectedClusterRef = useRef(selectedCluster);
  const selectedMarkerDataRef = useRef<MapMarker | null>(null);
  const onSelectClusterRef = useRef(onSelectCluster);
  const onSelectBranchRef = useRef(onSelectBranch);
  const [geocodedCoordinates, setGeocodedCoordinates] = useState<CoordinateMap>({});
  const [localInsight, setLocalInsight] = useState<InsightCardData | null>(null);
  const [mapLoaded, setMapLoaded] = useState(false);
  const mapboxToken = process.env.NEXT_PUBLIC_MAPBOX_TOKEN;

  const markerByClusterId = useMemo(() => {
    return new Map(markers.map((marker) => [marker.cluster_id, marker]));
  }, [markers]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    try {
      const raw = window.localStorage.getItem(GEO_CACHE_KEY);
      if (!raw) return;
      const parsed = JSON.parse(raw) as CoordinateMap;
      setGeocodedCoordinates(parsed);
    } catch {
      setGeocodedCoordinates({});
    }
  }, []);

  const resolveMarkerCoordinates = useCallback((marker?: MapMarker | null): [number, number] | null => {
    if (!marker) return null;
    if (typeof marker.longitude === "number" && typeof marker.latitude === "number") {
      return [marker.longitude, marker.latitude];
    }
    if (geocodedCoordinates[marker.cluster_id]) {
      return geocodedCoordinates[marker.cluster_id];
    }
    return fallbackCoordinates(marker);
  }, [geocodedCoordinates]);

  useEffect(() => {
    if (!mapboxToken) return;
    const pending = markers.filter((marker) => !resolveMarkerCoordinates(marker) && buildGeocodeQuery(marker));
    if (!pending.length) return;

    let cancelled = false;

    void (async () => {
      const updates: CoordinateMap = {};

      for (const marker of pending) {
        const query = buildGeocodeQuery(marker);
        if (!query) continue;

        try {
          const params = new URLSearchParams({
            access_token: mapboxToken,
            limit: "1",
            autocomplete: "false",
          });
          const response = await fetch(
            `https://api.mapbox.com/geocoding/v5/mapbox.places/${encodeURIComponent(query)}.json?${params.toString()}`,
          );
          if (!response.ok) continue;

          const payload = (await response.json()) as { features?: Array<{ center?: [number, number] }> };
          const center = payload.features?.[0]?.center;
          if (!center || center.length !== 2) continue;
          updates[marker.cluster_id] = center;
        } catch {
          continue;
        }
      }

      if (cancelled || !Object.keys(updates).length) return;

      setGeocodedCoordinates((current) => {
        const next = { ...current, ...updates };
        if (typeof window !== "undefined") {
          window.localStorage.setItem(GEO_CACHE_KEY, JSON.stringify(next));
        }
        return next;
      });
    })();

    return () => {
      cancelled = true;
    };
  }, [mapboxToken, markers, resolveMarkerCoordinates]);

  const selectedMarker = useMemo(
    () => markerByClusterId.get(selectedClusterId) ?? null,
    [markerByClusterId, selectedClusterId],
  );

  const selectedCoordinates = useMemo<[number, number]>(() => {
    return (
      resolveMarkerCoordinates(selectedMarker) ??
      fallbackCoordinates({
        cluster_id: selectedCluster.cluster_id,
        label: selectedCluster.subject_company_name,
        country: selectedCluster.subject_country,
      }) ??
      ([10, 49] as [number, number])
    );
  }, [resolveMarkerCoordinates, selectedCluster, selectedMarker]);

  useEffect(() => {
    setLocalInsight(buildSelectedEventCard(selectedCluster, selectedMarker));
  }, [selectedCluster, selectedMarker]);

  const positionedMarkers = useMemo(() => {
    const baseMarkers = items
      .map((item) => {
        const marker = markerByClusterId.get(item.cluster_id) ?? null;
        const baseCoordinates =
          resolveMarkerCoordinates(marker) ??
          fallbackCoordinates({
            cluster_id: item.cluster_id,
            label: item.subject_company_name,
            country: item.subject_country,
          });
        if (!baseCoordinates) return null;
        return { item, marker, baseCoordinates };
      })
      .filter(Boolean) as Array<{ item: OpportunitySummary; marker: MapMarker | null; baseCoordinates: [number, number] }>;

    const groups = new Map<string, typeof baseMarkers>();

    baseMarkers.forEach((entry) => {
      const key = `${entry.baseCoordinates[0].toFixed(3)}:${entry.baseCoordinates[1].toFixed(3)}`;
      const current = groups.get(key) ?? [];
      current.push(entry);
      groups.set(key, current);
    });

    return baseMarkers.map((entry) => {
      const key = `${entry.baseCoordinates[0].toFixed(3)}:${entry.baseCoordinates[1].toFixed(3)}`;
      const siblings = (groups.get(key) ?? []).sort((left, right) => left.item.cluster_id.localeCompare(right.item.cluster_id));
      const index = siblings.findIndex((sibling) => sibling.item.cluster_id === entry.item.cluster_id);
      const exactLocation = Boolean(entry.marker?.latitude || entry.marker?.longitude || entry.marker?.city || entry.marker?.state || entry.marker?.address);
      return {
        ...entry,
        coordinates: entry.item.cluster_id === selectedClusterId
          ? entry.baseCoordinates
          : spreadCoordinates(entry.baseCoordinates, Math.max(index, 0), siblings.length, exactLocation),
      } satisfies PositionedMarker;
    });
  }, [items, markerByClusterId, resolveMarkerCoordinates, selectedClusterId]);

  const backgroundFeatures = useMemo(() => {
    return positionedMarkers
      .filter((entry) => entry.item.cluster_id !== selectedClusterId)
      .map((entry) => ({
        type: "Feature" as const,
        geometry: {
          type: "Point" as const,
          coordinates: entry.coordinates,
        },
        properties: {
          clusterId: entry.item.cluster_id,
          label: entry.item.subject_company_name,
          triggerType: entry.item.trigger_type ?? "other",
          color: colorForTrigger(entry.item.trigger_type),
          country: entry.item.subject_country ?? "",
        },
      }));
  }, [positionedMarkers, selectedClusterId]);

  useEffect(() => {
    positionedMarkersRef.current = positionedMarkers;
  }, [positionedMarkers]);

  useEffect(() => {
    selectedClusterRef.current = selectedCluster;
  }, [selectedCluster]);

  useEffect(() => {
    selectedMarkerDataRef.current = selectedMarker;
  }, [selectedMarker]);

  useEffect(() => {
    onSelectClusterRef.current = onSelectCluster;
  }, [onSelectCluster]);

  useEffect(() => {
    onSelectBranchRef.current = onSelectBranch;
  }, [onSelectBranch]);

  useEffect(() => {
    if (!ref.current || !mapboxToken || mapRef.current) return;

    mapboxgl.accessToken = mapboxToken;

    const map = new mapboxgl.Map({
      container: ref.current,
      style: "mapbox://styles/mapbox/dark-v11",
      center: [10, 49],
      zoom: 3.6,
      attributionControl: false,
    });

    mapRef.current = map;
    map.addControl(new mapboxgl.NavigationControl({ showCompass: false }), "top-right");

    map.on("load", () => {
      map.setFog({
        color: "rgb(7, 12, 18)",
        "high-color": "rgb(20, 33, 52)",
        "space-color": "rgb(3, 6, 10)",
        "horizon-blend": 0.08,
      });

      map.addSource("background-events", {
        type: "geojson",
        data: {
          type: "FeatureCollection",
          features: [],
        },
        cluster: true,
        clusterRadius: 40,
        clusterMaxZoom: 7,
      });

      map.addLayer({
        id: "background-event-clusters",
        type: "circle",
        source: "background-events",
        filter: ["has", "point_count"],
        paint: {
          "circle-color": "rgba(112, 130, 150, 0.22)",
          "circle-stroke-color": "rgba(186, 201, 219, 0.18)",
          "circle-stroke-width": 1,
          "circle-radius": ["step", ["get", "point_count"], 14, 3, 18, 8, 24],
        },
      });

      map.addLayer({
        id: "background-event-count",
        type: "symbol",
        source: "background-events",
        filter: ["has", "point_count"],
        layout: {
          "text-field": ["get", "point_count_abbreviated"],
          "text-size": 11,
        },
        paint: {
          "text-color": "#d9e3ef",
        },
      });

      map.addLayer({
        id: "background-event-points",
        type: "circle",
        source: "background-events",
        filter: ["!", ["has", "point_count"]],
        paint: {
          "circle-color": ["get", "color"],
          "circle-radius": 5.5,
          "circle-opacity": 0.34,
          "circle-stroke-width": 1,
          "circle-stroke-color": "rgba(255,255,255,0.18)",
        },
      });

      map.on("click", "background-event-clusters", (event) => {
        const feature = event.features?.[0];
        if (!feature) return;
        const source = map.getSource("background-events") as GeoJSONSource;
        const clusterId = feature.properties?.cluster_id;
        source.getClusterExpansionZoom(clusterId, (error, zoom) => {
          if (error || typeof zoom !== "number") return;
          const coordinates = (feature.geometry as GeoJSON.Point).coordinates as [number, number];
          map.easeTo({ center: coordinates, zoom });
        });
      });

      map.on("click", "background-event-points", (event) => {
        const feature = event.features?.[0];
        const clusterId = feature?.properties?.clusterId;
        if (!clusterId) return;
        const entry = positionedMarkersRef.current.find((item) => item.item.cluster_id === String(clusterId));
        if (!entry) return;
        setLocalInsight(buildMarkerCard(entry.item, entry.marker));
        onSelectClusterRef.current(entry.item.cluster_id);
      });

      map.on("mouseenter", "background-event-points", () => {
        map.getCanvas().style.cursor = "pointer";
      });

      map.on("mouseleave", "background-event-points", () => {
        map.getCanvas().style.cursor = "";
      });

      map.on("mouseenter", "background-event-clusters", () => {
        map.getCanvas().style.cursor = "pointer";
      });

      map.on("mouseleave", "background-event-clusters", () => {
        map.getCanvas().style.cursor = "";
      });

      setMapLoaded(true);
    });

    return () => {
      setMapLoaded(false);
      branchMarkersRef.current.forEach((marker) => marker.remove());
      branchMarkersRef.current = [];
      selectedMarkerRef.current?.remove();
      selectedMarkerRef.current = null;
      mapRef.current = null;
      map.remove();
    };
  }, [mapboxToken]);

  useEffect(() => {
    if (!mapLoaded || !mapRef.current) return;
    const source = mapRef.current.getSource("background-events") as GeoJSONSource | undefined;
    if (!source) return;
    source.setData({
      type: "FeatureCollection",
      features: backgroundFeatures as GeoJSON.Feature[],
    });
  }, [backgroundFeatures, mapLoaded]);

  useEffect(() => {
    const map = mapRef.current;
    if (!mapLoaded || !map) return;

    const selectedButton = document.createElement("button");
    selectedButton.type = "button";
    selectedButton.className = "focusMarker";

    const pulse = document.createElement("div");
    pulse.className = "focusMarkerPulse";
    const core = document.createElement("div");
    core.className = "focusMarkerCore";
    const scoreSpan = document.createElement("span");
    scoreSpan.textContent = String(formatScore(selectedCluster.opportunity_score));
    core.appendChild(scoreSpan);
    selectedButton.appendChild(pulse);
    selectedButton.appendChild(core);

    selectedMarkerRef.current?.remove();

    const handleClick = () => {
      setLocalInsight(buildSelectedEventCard(selectedClusterRef.current, selectedMarkerDataRef.current));
    };
    selectedButton.addEventListener("click", handleClick);

    selectedMarkerRef.current = new mapboxgl.Marker(selectedButton).setLngLat(selectedCoordinates).addTo(map);
    map.flyTo({ center: selectedCoordinates, zoom: 4.3, duration: 700 });

    return () => {
      selectedButton.removeEventListener("click", handleClick);
      selectedMarkerRef.current?.remove();
      selectedMarkerRef.current = null;
    };
  }, [mapLoaded, selectedCluster.cluster_id, selectedCluster.opportunity_score, selectedCoordinates]);

  useEffect(() => {
    const map = mapRef.current;
    if (!mapLoaded || !map) return;
    branchMarkersRef.current.forEach((marker) => marker.remove());
    branchMarkersRef.current = [];

    branchMarkersRef.current = (["direct", "peer", "ownership"] as BranchKey[])
      .map((branch) => {
        const branchEntities = focusEntities.filter((entity) => entity.branch_type === branch);
        if (!branchEntities.length) return null;

        const meta = BRANCH_META[branch];
        const marker = document.createElement("button");
        marker.type = "button";
        marker.className = "branchMarker";

        const label = document.createElement("div");
        label.className = "branchMarkerLabel";
        label.style.borderColor = `${meta.color}66`;
        label.style.color = meta.color;
        label.style.background = `${meta.color}18`;
        const strong = document.createElement("strong");
        strong.textContent = formatLabel(branch);
        const countSpan = document.createElement("span");
        countSpan.textContent = String(branchEntities.length);
        label.appendChild(strong);
        label.appendChild(countSpan);
        marker.appendChild(label);

        const handleClick = () => {
          setLocalInsight(buildBranchCard(branch, branchEntities, selectedClusterRef.current));
          onSelectBranchRef.current(branch);
        };
        marker.addEventListener("click", handleClick);

        return new mapboxgl.Marker(marker).setLngLat(offsetPoint(selectedCoordinates, meta.angle, meta.radius)).addTo(map);
      })
      .filter(Boolean) as mapboxgl.Marker[];

    return () => {
      branchMarkersRef.current.forEach((marker) => marker.remove());
      branchMarkersRef.current = [];
    };
  }, [focusEntities, mapLoaded, selectedCluster.cluster_id, selectedCoordinates]);

  if (!mapboxToken) {
    return (
      <div className="panel mapCanvas" style={{ display: "grid", placeItems: "center", color: "var(--muted)" }}>
        Add <code>NEXT_PUBLIC_MAPBOX_TOKEN</code> to render the live map.
      </div>
    );
  }

  return (
    <div className="mapColumn">
      <div className="panel mapStage">
        <div ref={ref} className="mapCanvas" />
        <div className="mapOverlay mapOverlayTop">
          <div className="mapOverlayCard">
            <h2 className="panelTitle">Focused on {selectedCluster.subject_company_name}</h2>
            <RichTextBlock
              className="bodyText bodyTextMuted"
              text="The large anchor is the active trigger. Direct, peer, and ownership branches stay tied to that event, while all other triggers remain visible but muted. Pins use exact coordinates when available and otherwise fall back to the best available location context."
            />
            <div className="semanticLegend semanticLegendMap">
              {triggerLegendItems.map((item) => (
                <span key={item.key}>
                  <i style={{ background: item.color }} />
                  {item.label}
                </span>
              ))}
            </div>
            <div className="semanticLegend semanticLegendMap">
              {branchLegendItems.map((item) => (
                <span key={item.key}>
                  <i style={{ background: item.color }} />
                  {item.label}
                </span>
              ))}
            </div>
          </div>
        </div>
      </div>
      <SelectionInfoCard
        sticky={false}
        data={localInsight}
        emptyTitle="Map Detail"
        emptyBody="Select a trigger or branch on the map to inspect its event and opportunity context."
      />
    </div>
  );
}
