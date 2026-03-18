from __future__ import annotations

"""
aisql.py
--------
Translates natural language to SQL using HuggingFace Inference API.
Uses Llama-3.1-8B-Instruct via free inference endpoint.

The model receives a prompt describing the DB schema and the user's
question, and returns a SQL query.
"""

from huggingface_hub import InferenceClient


from catalog import Catalog


def build_schema_prompt(catalog: Catalog) -> str:
    """Build the AI system prompt from the actual live catalog."""
    tables = catalog.list_tables()

    if not tables:
        table_description = "The database has no tables yet."
    else:
        lines = []
        for table_name in tables:
            schema = catalog.get_schema(table_name)
            col_lines = ["  - id (integer, primary key, auto-increment)"]
            for col in schema.columns:
                col_lines.append(f"  - {col.name} ({col.col_type.lower()})")
            lines.append(f'Table "{table_name}":\n' + "\n".join(col_lines))
        table_description = "\n\n".join(lines)

    return f"""You are a SQL expert. You help users query a database.

{table_description}

Supported SQL:
  SELECT * FROM <table>
  SELECT * FROM <table> WHERE id = <number>
  SELECT * FROM <table> WHERE <column> > <number>
  SELECT * FROM <table> WHERE <column> < <number>
  SELECT <col1>, <col2> FROM <table>
  INSERT INTO <table> (<col1>, <col2>) VALUES (<val1>, <val2>)
  DELETE FROM <table> WHERE id = <number>
  UPDATE <table> SET <col> = <val> WHERE id = <number>
  SELECT * FROM <table1> JOIN <table2> ON <table1>.id = <table2>.<fk>

Rules:
  - Return ONLY the SQL query, nothing else
  - No explanations, no markdown, no backticks
  - Always use lowercase for SQL keywords
  - If you cannot translate the request, return: UNKNOWN
"""


class AITranslator:
    def __init__(self, hf_token: str, catalog: Catalog):
        self.client = InferenceClient(
            model="meta-llama/Llama-3.1-8B-Instruct",
            token=hf_token,
            
        )
        self.catalog = catalog 
    def _get_schema(self) -> str:
        """Always build fresh from current catalog state."""
        return build_schema_prompt(self.catalog)  
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
{self._get_schema()}

When given a failed SQL query and its error, explain what went wrong
in simple terms and suggest the correct query.
Be concise -- maximum 3 lines."""
                    },
                    {
                        "role": "user",
                        "content": f"This SQL failed:\n{sql}\n\nError: {error}\n\nWhat went wrong and how do I fix it?"
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
            {"role": "system", "content": {self._get_schema()}},
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
