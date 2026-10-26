import os
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
MODEL = "openai/gpt-oss-20b"

client = Groq(api_key=GROQ_API_KEY)

SYSTEM_PROMPT = """You are a clinical decision support assistant for MediCore, 
a real-time patient monitoring system. You help doctors interpret patient vitals data.

When given patient vitals, you:
1. Identify concerning patterns or trends
2. Flag critical values that need immediate attention  
3. Provide concise, clinically relevant observations
4. Always recommend consulting specialists for definitive diagnoses

You are an assistant tool, not a replacement for clinical judgment.
Keep responses concise and actionable — doctors are busy."""


def ask_clinical_question(question: str, vitals_context: list[dict]) -> str:
    vitals_summary = "\n".join([
        f"- {v['recorded_at']}: HR={v['heart_rate']} bpm, "
        f"O2={v['oxygen_level']}%, "
        f"BP={v['blood_pressure_systolic']}/{v['blood_pressure_diastolic']} mmHg"
        for v in vitals_context
    ])

    user_message = f"""Patient vitals (last 10 readings, most recent first):
    {vitals_summary} 

    Doctor's question: {question}"""

    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        max_tokens=500,
        temperature=0.3,
    )

    return response.choices[0].message.content