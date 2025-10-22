import { SignalsDashboard, type MarketSignal } from "./components/SignalsDashboard";

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";
const DEFAULT_MARKET = process.env.NEXT_PUBLIC_DEFAULT_MARKET ?? "salt_lake_county";
const DEFAULT_LIMIT = 200;

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


export default async function Home() {
  const response = await fetchSignals(DEFAULT_MARKET);
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

        <SignalsDashboard
          signals={response.items}
          downloadBase={downloadBase}
          errorMessage={errorMessage}
          totalCount={response.count}
        />
      </main>
    </div>
  );
}
