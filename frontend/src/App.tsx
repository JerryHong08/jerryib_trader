import { useEffect, useState, useRef } from "react";

export default function OrderBookDashboard() {
  const [symbols, setSymbols] = useState<string[]>(() => {
    const saved = localStorage.getItem("symbols");
    return saved ? JSON.parse(saved) : [];
  });
  const [input, setInput] = useState("");
  const ws = useRef<WebSocket | null>(null);

  useEffect(() => {
    ws.current = new WebSocket("ws://localhost:8000/ws/quotes");

    ws.current.onopen = () => {
      console.log("✅ Connected to backend");
      ws.current?.send(JSON.stringify({ symbols }));
    };

    ws.current.onmessage = (event) => {
      console.log("📩 Message from server:", event.data);
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
  };

  return (
    <div className="p-4">
      <div className="flex gap-2">
        <input
          className="border p-2"
          placeholder="Enter tickers, e.g. AAPL, NVDA"
          value={input}
          onChange={(e) => setInput(e.target.value)}
        />
        <button onClick={addSymbol} className="bg-blue-500 text-white px-4 py-2 rounded">
          Add
        </button>
      </div>

      <div className="mt-4 space-y-2">
        {symbols.map((s) => (
          <div key={s} className="flex justify-between border p-2 rounded">
            <span>{s}</span>
            <button
              className="text-red-500"
              onClick={() => removeSymbol(s)}
            >
              ✕
            </button>
          </div>
        ))}
      </div>
    </div>
  );
}
