"""
ai_translator.py
----------------
Translates natural language to SQL using HuggingFace Inference API.
Uses Mistral-7B-Instruct — free, no local GPU needed.

The model receives a prompt describing your DB schema and the user's
question, and returns a SQL query.
"""

from huggingface_hub import InferenceClient


# Your table schema — tell the model exactly what exists
SCHEMA = """
You are a SQL expert. You help users query a database.

The database has ONE table called "users" with these columns:
  - id   (integer, primary key, auto-increment)
  - name (text, max 20 characters)
  - age  (integer)

Supported SQL:
  SELECT * FROM users
  SELECT * FROM users WHERE id = <number>
  SELECT * FROM users WHERE age > <number>
  SELECT * FROM users WHERE age < <number>
  SELECT name FROM users
  SELECT name, age FROM users
  INSERT INTO users (name, age) VALUES ("<name>", <age>)
  DELETE FROM users WHERE id = <number>

Rules:
  - Return ONLY the SQL query, nothing else
  - No explanations, no markdown, no backticks
  - Always use lowercase for SQL keywords
  - If you cannot translate the request, return: UNKNOWN
"""


from huggingface_hub import InferenceClient

SCHEMA = """
You are a SQL expert. You help users query a database.

The database has ONE table called "users" with these columns:
  - id   (integer, primary key, auto-increment)
  - name (text, max 20 characters)
  - age  (integer)

Supported SQL:
  SELECT * FROM users
  SELECT * FROM users WHERE id = <number>
  SELECT * FROM users WHERE age > <number>
  SELECT * FROM users WHERE age < <number>
  SELECT name FROM users
  SELECT name, age FROM users
  INSERT INTO users (name, age) VALUES ("<name>", <age>)
  DELETE FROM users WHERE id = <number>

Rules:
  - Return ONLY the SQL query, nothing else
  - No explanations, no markdown, no backticks
  - Always use lowercase for SQL keywords
  - If you cannot translate the request, return: UNKNOWN
"""


class AITranslator:
    def __init__(self, hf_token: str):
        self.client = InferenceClient(
            model="meta-llama/Llama-3.1-8B-Instruct",
            token=hf_token,
        )
    def explain_error(self, sql: str, error: str) -> str:
        """
        Takes the SQL that failed and the error message.
        Returns a human-friendly explanation with a suggestion.
        """
        try:
            response = self.client.chat_completion(
                messages=[
                    {
                        "role": "system",
                        "content": f"""You are a helpful database assistant.
    The user is using a simple database with this schema:
    {SCHEMA}

    When given a failed SQL query and its error, explain what went wrong
    in simple terms and suggest the correct query.
    Be concise — maximum 3 lines."""
                    },
                    {
                        "role": "user",
                        "content": f"This SQL failed:\\n{sql}\\n\\nError: {error}\\n\\nWhat went wrong and how do I fix it?"
                    }
                ],
                max_tokens=150,
                temperature=0.3,
            )
            return response.choices[0].message.content.strip()

        except Exception as e:
            return f"Could not explain error: {e}"

    def translate(self, natural_language: str) -> str | None:
        messages = [
            {"role": "system", "content": SCHEMA},
            {"role": "user",   "content": natural_language},
        ]

        try:
            response = self.client.chat_completion(
                messages=messages,
                max_tokens=100,
                temperature=0.1,
            )

            sql = response.choices[0].message.content.strip()

            # Clean up any markdown the model might add
            sql = sql.strip("`").strip()
            if sql.startswith("sql"):
                sql = sql[3:].strip()

            if not sql or sql.upper() == "UNKNOWN":
                return None

            return sql

        except Exception as e:
            print(f"AI error: {e}")
            return None
# ------------------------------------------------------------------ #
#  Quick test                                                          #
# ------------------------------------------------------------------ #
if __name__ == "__main__":
    import os
    token = os.environ.get("HF_TOKEN", "your_token_here")
    ai = AITranslator(token)

    tests = [
        "show me all users",
        "find user with id 3",
        "add a user named Bob who is 25 years old",
        "delete user number 5",
        "who is older than 30",
        "show me just the names",
    ]

    for t in tests:
        sql = ai.translate(t)
        print(f"Input:  {t}")
        print(f"SQL:    {sql}")
        print()
