import os
from openai import AzureOpenAI
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_ENDPOINT = os.getenv("OPENAI_API_ENDPOINT")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")
OPENAI_MODEL = os.getenv("AZURE_OPENAI_API_DEPLOYMENT_NAME", "gpt-4o-mini")


def _summary_prompt(prev_summary: str | None) -> str:
    if prev_summary:
        return (
            f"This is summary of the conversation to date: {prev_summary}\n\n"
            "Extend the summary by taking into account the new messages above, as short as possible:"
        )
    else:
        return "Create a summary of the conversation above, as short as possible:"


def summarize_conversation(messages: list[dict], prev_summary: str | None) -> str:
    client = AzureOpenAI(
        azure_endpoint=OPENAI_API_ENDPOINT,
        api_key=OPENAI_API_KEY,
        api_version=OPENAI_API_VERSION,
    )

    summary_message = _summary_prompt(prev_summary)

    all_messages = [*messages, {"role": "system", "content": summary_message}]

    # Normalise message format: LangChain-style 'type' key → OpenAI 'role' key
    openai_messages = []
    for msg in all_messages:
        role = msg.get("role") or msg.get("type")
        if role == "human":
            role = "user"
        elif role == "ai":
            role = "assistant"
        content = msg.get("content", "")
        if isinstance(content, list):
            # Flatten content blocks to plain text
            content = " ".join(
                block.get("text", "") for block in content if isinstance(block, dict)
            )
        openai_messages.append({"role": role, "content": content})

    res = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=openai_messages,
    )
    return res.choices[0].message.content
