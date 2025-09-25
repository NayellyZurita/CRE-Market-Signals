import Link from "next/link";

import { SignalsChart } from "./components/SignalsChart";

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";
const DEFAULT_MARKET = process.env.NEXT_PUBLIC_DEFAULT_MARKET ?? "salt_lake_county";
const DEFAULT_LIMIT = 200;

type MarketSignal = {
  source: string;
  geo_level: string;
  geo_id: string;
  geo_name: string;
  observed_at: string;
  metric: string;
  value: number;
  unit: string;
};

type ApiResponse = {
  count: number;
  items: MarketSignal[];
  error?: string;
};

async function fetchSignals(marketKey: string): Promise<ApiResponse> {
  const url = `${API_BASE_URL}/signals?market=${marketKey}&limit=${DEFAULT_LIMIT}`;
  try {
    const res = await fetch(url, {
      cache: "no-store",
      next: { revalidate: 0 },
    });

    if (!res.ok) {
      throw new Error(`Failed to load signals from API (${res.status})`);
    }

    return res.json();
  } catch (error) {
    console.error("Failed to fetch signals", error);
    const message =
      error instanceof Error ? error.message : "Unable to reach the API endpoint.";
    return { count: 0, items: [], error: message };
  }
}

function buildChartSeries(signals: MarketSignal[]) {
  if (signals.length === 0) {
    return { metric: "", unit: "", data: [] as { observed_at: string; value: number }[] };
  }

  const preferredMetric =
    signals.find((signal) => signal.metric === "fmr_2br")?.metric ?? signals[0]!.metric;

  const metricSignals = signals
    .filter((signal) => signal.metric === preferredMetric)
    .map((signal) => ({
      observed_at: signal.observed_at,
      value: signal.value,
    }))
    .sort((a, b) => new Date(a.observed_at).getTime() - new Date(b.observed_at).getTime());

  const unit = signals.find((signal) => signal.metric === preferredMetric)?.unit ?? "";

  return {
    metric: preferredMetric,
    unit,
    data: metricSignals,
  };
}

export default async function Home() {
  const response = await fetchSignals(DEFAULT_MARKET);
  const chartSeries = buildChartSeries(response.items);
  const downloadBase = `${API_BASE_URL}/signals?market=${DEFAULT_MARKET}&limit=${DEFAULT_LIMIT}`;
  const errorMessage = response.error;

  return (
    <div className="min-h-screen bg-slate-50 text-slate-900">
      <main className="mx-auto flex max-w-5xl flex-col gap-10 px-6 py-10">
        <header className="flex flex-col gap-2">
          <h1 className="text-3xl font-semibold">CRE Market Signals</h1>
          <p className="text-slate-600">
            Unified metrics sourced from HUD, ACS, and FRED for {response.items[0]?.geo_name ?? "your market"}.
            Export the latest dataset in your preferred format or explore the quick chart below.
          </p>
        </header>

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
          <div className="mb-4 flex items-baseline justify-between">
            <div>
              <h2 className="text-xl font-semibold">{chartSeries.metric || "No data"}</h2>
              <p className="text-sm text-slate-500">Unit: {chartSeries.unit || "n/a"}</p>
            </div>
            <p className="text-sm text-slate-500">Total records fetched: {response.count}</p>
          </div>
          {errorMessage ? (
            <p className="text-sm text-red-500">
              {errorMessage}. Ensure the API is running at {API_BASE_URL}.
            </p>
          ) : chartSeries.data.length > 0 ? (
            <SignalsChart data={chartSeries.data} metric={chartSeries.metric} unit={chartSeries.unit} />
          ) : (
            <p className="text-sm text-slate-500">No chartable data available for the selected market.</p>
          )}
        </section>

        <section className="rounded-lg bg-white p-6 shadow">
          <h2 className="mb-3 text-lg font-semibold">Recent observations</h2>
          <ul className="grid gap-3 sm:grid-cols-2">
            {response.items.slice(0, 6).map((signal) => (
              <li key={`${signal.metric}-${signal.observed_at}-${signal.geo_id}`} className="rounded border border-slate-200 p-3">
                <p className="text-sm font-medium text-slate-700">{signal.metric}</p>
                <p className="text-2xl font-semibold text-slate-900">
                  {signal.value.toLocaleString(undefined, { maximumFractionDigits: 2 })} {signal.unit}
                </p>
                <p className="text-xs text-slate-500">Observed {new Date(signal.observed_at).toLocaleDateString()}</p>
                <p className="text-xs text-slate-400">Source: {signal.source}</p>
              </li>
            ))}
          </ul>
        </section>
      </main>
    </div>
  );
}
