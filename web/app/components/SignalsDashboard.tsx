"use client";

import { useMemo, useState, useEffect } from "react";
import Link from "next/link";

import { SignalsChart } from "./SignalsChart";

export type MarketSignal = {
  source: string;
  geo_level: string;
  geo_id: string;
  geo_name: string;
  observed_at: string;
  metric: string;
  value: number;
  unit: string;
};

type SignalsDashboardProps = {
  signals: MarketSignal[];
  downloadBase: string;
  errorMessage?: string;
  totalCount: number;
};

type MetricOption = {
  metric: string;
  unit: string;
  source: string;
};

function buildMetricOptions(signals: MarketSignal[]): MetricOption[] {
  const map = new Map<string, MetricOption>();
  for (const signal of signals) {
    if (!map.has(signal.metric)) {
      map.set(signal.metric, {
        metric: signal.metric,
        unit: signal.unit,
        source: signal.source,
      });
    }
  }
  return Array.from(map.values()).sort((a, b) => a.metric.localeCompare(b.metric));
}

function buildLatestObservations(signals: MarketSignal[]): MarketSignal[] {
  const sorted = [...signals].sort(
    (a, b) => new Date(b.observed_at).getTime() - new Date(a.observed_at).getTime()
  );
  const byMetric = new Map<string, MarketSignal>();
  for (const signal of sorted) {
    if (!byMetric.has(signal.metric)) {
      byMetric.set(signal.metric, signal);
    }
  }
  return Array.from(byMetric.values()).sort((a, b) => a.metric.localeCompare(b.metric));
}

function buildChartSeries(signals: MarketSignal[], metric: string) {
  if (!metric) {
    return { metric: "", unit: "", data: [] as { observed_at: string; value: number }[] };
  }
  const metricSignals = signals
    .filter((signal) => signal.metric === metric)
    .map((signal) => ({ observed_at: signal.observed_at, value: signal.value }))
    .sort((a, b) => new Date(a.observed_at).getTime() - new Date(b.observed_at).getTime());
  const unit = signals.find((signal) => signal.metric === metric)?.unit ?? "";
  return { metric, unit, data: metricSignals };
}

export function SignalsDashboard({
  signals,
  downloadBase,
  errorMessage,
  totalCount,
}: SignalsDashboardProps) {
  const metricOptions = useMemo(() => buildMetricOptions(signals), [signals]);

  const [selectedMetric, setSelectedMetric] = useState<string>(() => {
    const preferred = metricOptions.find((option) => option.metric === "fmr_2br");
    return preferred?.metric ?? metricOptions[0]?.metric ?? "";
  });

  useEffect(() => {
    if (!metricOptions.some((option) => option.metric === selectedMetric)) {
      setSelectedMetric(metricOptions[0]?.metric ?? "");
    }
  }, [metricOptions, selectedMetric]);

  const chartSeries = useMemo(
    () => buildChartSeries(signals, selectedMetric),
    [signals, selectedMetric]
  );

  const latestObservations = useMemo(
    () => buildLatestObservations(signals),
    [signals]
  );

  const selectedOption = metricOptions.find((option) => option.metric === selectedMetric);

  return (
    <>
      <section className="flex flex-wrap gap-4">
        <a
          className="rounded-full bg-blue-600 px-4 py-2 text-sm font-medium text-white shadow hover:bg-blue-700"
          href={`${downloadBase}&format=json`}
          target="_blank"
          rel="noopener noreferrer"
        >
          Download JSON
        </a>
        <a
          className="rounded-full bg-emerald-600 px-4 py-2 text-sm font-medium text-white shadow hover:bg-emerald-700"
          href={`${downloadBase}&format=csv`}
          target="_blank"
          rel="noopener noreferrer"
        >
          Download CSV
        </a>
        <a
          className="rounded-full bg-fuchsia-600 px-4 py-2 text-sm font-medium text-white shadow hover:bg-fuchsia-700"
          href={`${downloadBase}&format=parquet`}
          target="_blank"
          rel="noopener noreferrer"
        >
          Download Parquet
        </a>
        <Link
          className="rounded-full border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-white"
          href="/"
        >
          Refresh Dashboard
        </Link>
      </section>

      <section className="rounded-lg bg-white p-6 shadow">
        <div className="mb-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div className="flex flex-col gap-1 sm:flex-row sm:items-center sm:gap-3">
            <label htmlFor="metric-select" className="text-sm font-medium text-slate-700">
              Metric
            </label>
            <select
              id="metric-select"
              className="rounded border border-slate-300 px-3 py-2 text-sm"
              value={selectedMetric}
              onChange={(event) => setSelectedMetric(event.target.value)}
              aria-label="Select metric to visualize"
            >
              {metricOptions.map((option) => (
                <option key={option.metric} value={option.metric}>
                  {option.metric} ({option.source})
                </option>
              ))}
            </select>
          </div>
          <p className="text-sm text-slate-500">Total records fetched: {totalCount}</p>
        </div>
        {errorMessage ? (
          <p className="text-sm text-red-500">
            {errorMessage}. Ensure the API is running at the configured host.
          </p>
        ) : chartSeries.data.length > 0 ? (
          <div className="flex flex-col gap-2">
            <p className="text-sm text-slate-500">
              Source: {selectedOption?.source ?? "?"} · Unit: {selectedOption?.unit || "n/a"}
            </p>
            <SignalsChart data={chartSeries.data} metric={chartSeries.metric} unit={chartSeries.unit} />
          </div>
        ) : (
          <p className="text-sm text-slate-500">No chartable data available for the selected metric.</p>
        )}
      </section>

      <section className="rounded-lg bg-white p-6 shadow">
        <h2 className="mb-3 text-lg font-semibold">Recent observations</h2>
        <ul className="grid gap-3 sm:grid-cols-2">
          {latestObservations.map((signal) => (
            <li
              key={`${signal.metric}-${signal.observed_at}-${signal.geo_id}`}
              className="rounded border border-slate-200 p-3"
            >
              <p className="text-sm font-medium text-slate-700">{signal.metric}</p>
              <p className="text-2xl font-semibold text-slate-900">
                {signal.value.toLocaleString(undefined, { maximumFractionDigits: 2 })} {signal.unit}
              </p>
              <p className="text-xs text-slate-500">
                Observed {new Date(signal.observed_at).toLocaleDateString()}
              </p>
              <p className="text-xs text-slate-400">Source: {signal.source}</p>
            </li>
          ))}
        </ul>
      </section>
    </>
  );
}
