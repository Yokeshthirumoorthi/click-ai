"""
LLM Agent: interactive CLI for querying traces using Claude + tool use.
"""

import sys

import anthropic

import config
from prompts import SYSTEM_PROMPT
from tools import TOOL_DEFINITIONS, execute_tool


def run_agent_loop(client: anthropic.Anthropic, messages: list):
    """Run one turn of the agent loop, handling tool calls."""
    while True:
        response = client.messages.create(
            model=config.MODEL,
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=TOOL_DEFINITIONS,
            messages=messages,
        )

        # Collect assistant content
        assistant_content = response.content
        messages.append({"role": "assistant", "content": assistant_content})

        # Check if we need to handle tool calls
        tool_uses = [b for b in assistant_content if b.type == "tool_use"]

        if not tool_uses:
            # No tool calls — print text response and return
            for block in assistant_content:
                if hasattr(block, "text"):
                    print(f"\n{block.text}")
            return

        # Execute tool calls and build results
        tool_results = []
        for tool_use in tool_uses:
            print(f"\n  [tool] {tool_use.name}({tool_use.input})")
            result = execute_tool(tool_use.name, tool_use.input)
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tool_use.id,
                "content": result,
            })

        messages.append({"role": "user", "content": tool_results})

        # Print any text blocks from this turn
        for block in assistant_content:
            if hasattr(block, "text") and block.text:
                print(f"\n{block.text}")


def main():
    if not config.ANTHROPIC_API_KEY:
        print("Error: ANTHROPIC_API_KEY not set")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
    messages = []

    print("Trace Agent (powered by Claude)")
    print("Type your question, or 'quit' to exit.\n")

    while True:
        try:
            user_input = input("you> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
            break

        if not user_input:
            continue
        if user_input.lower() in ("quit", "exit", "q"):
            print("Bye!")
            break

        messages.append({"role": "user", "content": user_input})

        try:
            run_agent_loop(client, messages)
        except anthropic.APIError as e:
            print(f"\nAPI error: {e}")
        except Exception as e:
            print(f"\nError: {e}")


if __name__ == "__main__":
    main()
