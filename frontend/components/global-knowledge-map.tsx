"use client";

import { useEffect, useMemo, useRef } from "react";
import mapboxgl, { GeoJSONSource, LngLatBoundsLike } from "mapbox-gl";

import { MapMarker } from "@/types/api";

type Props = {
  markers: MapMarker[];
  focusLabel: string;
};

const COUNTRY_COORDINATES: Record<string, [number, number]> = {
  Argentina: [-63.62, -38.42],
  Australia: [133.78, -25.27],
  Austria: [14.55, 47.52],
  Belgium: [4.47, 50.5],
  Brazil: [-51.93, -14.24],
  Canada: [-106.35, 56.13],
  China: [104.2, 35.86],
  France: [2.21, 46.23],
  Germany: [10.45, 51.17],
  India: [78.96, 20.59],
  Italy: [12.57, 41.87],
  Japan: [138.25, 36.2],
  Mexico: [-102.55, 23.63],
  Netherlands: [5.29, 52.13],
  Singapore: [103.82, 1.35],
  "South Africa": [22.94, -30.56],
  Spain: [-3.75, 40.46],
  Switzerland: [8.23, 46.82],
  "United Arab Emirates": [53.85, 23.42],
  "United Kingdom": [-2.5, 54.8],
  "United States": [-98.58, 39.83],
};

function fallbackCoordinates(marker: MapMarker): [number, number] | null {
  if (typeof marker.longitude === "number" && typeof marker.latitude === "number") {
    return [marker.longitude, marker.latitude];
  }
  if (marker.country && COUNTRY_COORDINATES[marker.country]) {
    return COUNTRY_COORDINATES[marker.country];
  }
  return null;
}

function buildBounds(points: [number, number][]): LngLatBoundsLike | null {
  if (!points.length) return null;
  const bounds = new mapboxgl.LngLatBounds(points[0], points[0]);
  for (const point of points.slice(1)) {
    bounds.extend(point);
  }
  return bounds;
}

export function GlobalKnowledgeMap({ markers, focusLabel }: Props) {
  const ref = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<mapboxgl.Map | null>(null);
  const mapboxToken = process.env.NEXT_PUBLIC_MAPBOX_TOKEN;

  const resolvedMarkers = useMemo(
    () =>
      markers
        .map((marker) => {
          const coordinates = fallbackCoordinates(marker);
          if (!coordinates) return null;
          return {
            ...marker,
            coordinates,
          };
        })
        .filter(Boolean) as Array<MapMarker & { coordinates: [number, number] }>,
    [markers],
  );

  useEffect(() => {
    if (!mapboxToken || !ref.current || mapRef.current) return;
    mapboxgl.accessToken = mapboxToken;

    const map = new mapboxgl.Map({
      container: ref.current,
      style: "mapbox://styles/mapbox/dark-v11",
      center: [8, 22],
      zoom: 1.2,
      attributionControl: false,
    });

    map.addControl(new mapboxgl.NavigationControl({ showCompass: false }), "top-right");
    map.on("load", () => {
      map.addSource("global-knowledge-focus", {
        type: "geojson",
        data: {
          type: "FeatureCollection",
          features: [],
        },
      });

      map.addLayer({
        id: "global-knowledge-focus-circles",
        type: "circle",
        source: "global-knowledge-focus",
        paint: {
          "circle-radius": 8,
          "circle-color": "#3f92ff",
          "circle-stroke-width": 2,
          "circle-stroke-color": "#f8fafc",
          "circle-opacity": 0.88,
        },
      });
    });

    mapRef.current = map;

    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, [mapboxToken]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !map.isStyleLoaded()) return;
    const source = map.getSource("global-knowledge-focus") as GeoJSONSource | undefined;
    if (!source) return;

    source.setData({
      type: "FeatureCollection",
      features: resolvedMarkers.map((marker) => ({
        type: "Feature",
        properties: {
          label: marker.label,
          country: marker.country ?? "",
        },
        geometry: {
          type: "Point",
          coordinates: marker.coordinates,
        },
      })),
    });

    const points = resolvedMarkers.map((marker) => marker.coordinates);
    const bounds = buildBounds(points);
    if (bounds) {
      map.fitBounds(bounds, {
        padding: 48,
        maxZoom: points.length <= 1 ? 4.8 : 5.5,
        duration: 900,
      });
    }
  }, [resolvedMarkers]);

  if (!mapboxToken) {
    return (
      <section className="panel detailCard globalMapFallback">
        <h2 className="panelTitle">Geographic Focus</h2>
        <p className="bodyText bodyTextMuted">
          Mapbox is not configured, so this workspace is using geographic summaries instead of the live map canvas.
        </p>
        <ul className="keyValueList keyValueListCompact">
          <li>
            <span>Current focus</span>
            <strong>{focusLabel}</strong>
          </li>
          <li>
            <span>Mapped events</span>
            <strong>{resolvedMarkers.length}</strong>
          </li>
        </ul>
      </section>
    );
  }

  return (
    <section className="panel detailCard">
      <div className="globalMapHeader">
        <div>
          <h2 className="panelTitle">Geographic Focus</h2>
          <p className="bodyText bodyTextMuted">The map automatically reframes as you move from region to country level.</p>
        </div>
        <span className="chip chip-blue">{focusLabel}</span>
      </div>
      <div ref={ref} className="globalKnowledgeMapCanvas" />
    </section>
  );
}
