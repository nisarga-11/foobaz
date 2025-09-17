#!/bin/bash
echo "🧪 RECOMMENDATION TOOL TEST SCRIPT"
echo "=================================="
echo ""
echo "Prerequisites:"
echo "1. Ollama server should be running"
echo "2. MCP servers should be running (./start_complete_system.sh)"
echo ""
echo "Starting system check..."
echo ""

# Check if Ollama is running
if curl -s http://localhost:11434/api/tags > /dev/null 2>&1; then
    echo "✅ Ollama: Running"
else
    echo "❌ Ollama: Not running - please start with 'ollama serve'"
    echo "   The tool will still work but use fallback recommendations"
fi

# Check if MCP servers are running
if curl -s http://localhost:8003/health > /dev/null 2>&1; then
    echo "✅ MCP1 Server: Running on port 8003"
else
    echo "❌ MCP1 Server: Not running on port 8003"
fi

if curl -s http://localhost:8004/health > /dev/null 2>&1; then
    echo "✅ MCP2 Server: Running on port 8004"
else
    echo "❌ MCP2 Server: Not running on port 8004"
fi

echo ""
echo "🎯 READY TO TEST!"
echo ""
echo "Copy and paste these prompts into the CLI or agent:"
echo ""
echo '1. "Generate 3 restore recommendations for all databases"'
echo ""
echo '2. "I need restore recommendations for timestamp 2025-09-13T20:30:00"'
echo ""
echo '3. "Show me the best restore points from recent backups"'
echo ""
echo "Or run: python cli.py"
echo "Then use natural language with these prompts!"
