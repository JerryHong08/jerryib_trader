import { useEffect, useState, useRef } from "react";

interface Quote {
  symbol: string;
  bid: number;
  ask: number;
  bid_size: number;
  ask_size: number;
  timestamp: number;
}

export default function OrderBookDashboard() {
  const [symbols, setSymbols] = useState<string[]>(() => {
    const saved = localStorage.getItem("symbols");
    return saved ? JSON.parse(saved) : [];
  });

  const [input, setInput] = useState("");
  const ws = useRef<WebSocket | null>(null);
  // 改为存储所有 symbols 的 quotes
  const [quotes, setQuotes] = useState<Record<string, Quote>>({});

  useEffect(() => {
    ws.current = new WebSocket("ws://localhost:8000/ws/quotes");

    ws.current.onopen = () => {
      console.log("✅ Connected to backend");
      ws.current?.send(JSON.stringify({ symbols }));
    };

    ws.current.onmessage = (event) => {
      console.log("📩 Message from server:", event.data);
      const data: Quote = JSON.parse(event.data);

      // 更新对应 symbol 的 quote
      setQuotes(prev => ({
        ...prev,
        [data.symbol]: data
      }));
    };

    ws.current.onclose = () => console.log("❌ Disconnected");

    return () => ws.current?.close();
  }, []);

  const addSymbol = () => {
    const newSymbols = [...new Set([...symbols, ...input.split(",").map(s => s.trim().toUpperCase())])];
    setSymbols(newSymbols);
    localStorage.setItem("symbols", JSON.stringify(newSymbols));
    ws.current?.send(JSON.stringify({ symbols: newSymbols }));
    setInput("");
  };

  const removeSymbol = (symbol: string) => {
    const updated = symbols.filter(s => s !== symbol);
    setSymbols(updated);
    localStorage.setItem("symbols", JSON.stringify(updated));
    ws.current?.send(`unsubscribe:${symbol}`);

    // 清除对应的 quote 数据
    setQuotes(prev => {
      const newQuotes = { ...prev };
      delete newQuotes[symbol];
      return newQuotes;
    });
  };

  return (
    <div className="p-4">
      <div className="flex gap-2 mb-4">
        <input
          className="border p-2 flex-1"
          placeholder="Enter tickers, e.g. AAPL, NVDA"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyPress={(e) => e.key === 'Enter' && addSymbol()}
        />
        <button
          onClick={addSymbol}
          className="bg-blue-500 text-white px-4 py-2 rounded hover:bg-blue-600"
        >
          Add
        </button>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        {symbols.map((symbol) => {
          const quote = quotes[symbol];
          return (
            <div key={symbol} className="border p-4 rounded-lg shadow-sm bg-gray-50">
              <div className="flex justify-between items-start mb-3">
                <h3 className="text-lg font-bold text-gray-800">{symbol}</h3>
                <button
                  className="text-red-500 hover:text-red-700 text-xl leading-none"
                  onClick={() => removeSymbol(symbol)}
                  title={`Remove ${symbol}`}
                >
                  ✕
                </button>
              </div>

              {quote ? (
                <div className="space-y-2">
                  <div className="flex justify-between">
                    <span className="text-sm text-gray-600">Bid:</span>
                    <span className="font-mono">
                      <span className="text-green-600 font-semibold">${quote.bid}</span>
                      <span className="text-gray-500 ml-1">x {quote.bid_size}</span>
                    </span>
                  </div>

                  <div className="flex justify-between">
                    <span className="text-sm text-gray-600">Ask:</span>
                    <span className="font-mono">
                      <span className="text-red-600 font-semibold">${quote.ask}</span>
                      <span className="text-gray-500 ml-1">x {quote.ask_size}</span>
                    </span>
                  </div>

                  <div className="flex justify-between">
                    <span className="text-sm text-gray-600">Spread:</span>
                    <span className="font-mono text-gray-700">
                      ${(quote.ask - quote.bid).toFixed(2)}
                    </span>
                  </div>

                  <div className="flex justify-between">
                    <span className="text-sm text-gray-600">Updated:</span>
                    <span className="text-xs text-gray-500">
                      {new Date(quote.timestamp / 1e6).toLocaleTimeString()}
                    </span>
                  </div>
                </div>
              ) : (
                <div className="flex items-center justify-center py-8">
                  <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-500"></div>
                  <span className="ml-2 text-gray-500">Waiting for data...</span>
                </div>
              )}
            </div>
          );
        })}
      </div>

      {symbols.length === 0 && (
        <div className="text-center py-12 text-gray-500">
          <p className="text-xl">No symbols added yet</p>
          <p>Add some stock symbols to start monitoring quotes</p>
        </div>
      )}
    </div>
  );
}
