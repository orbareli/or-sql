import cmd
import os
import time

from rich.console import Console
from rich.table import Table as RichTable
from rich.panel import Panel
from rich.progress import track
from rich.text import Text
from rich import box
from dotenv import load_dotenv

from catalog import Catalog
from executor import Executor
from aisql import AITranslator

console = Console()


class TerminalUI:

    @staticmethod
    def banner():
        console.print(
            Panel.fit(
                "[bold green]orSQL Engine[/bold green]\n[dim]Custom Python Storage Engine[/dim]",
                border_style="green"
            )
        )

    @staticmethod
    def success(message):
        console.print(f"[bold green]{message}[/bold green]")

    @staticmethod
    def error(message):
        console.print(f"[bold red]{message}[/bold red]")

    @staticmethod
    def info(message):
        console.print(f"[cyan]{message}[/cyan]")

    @staticmethod
    def print_table(rows):
        if not rows:
            console.print("[yellow](0 rows)[/yellow]")
            return

        table = RichTable(box=box.SIMPLE_HEAVY)
        for key in rows[0].keys():
            table.add_column(str(key), style="green")
        for row in rows:
            table.add_row(*[str(v) for v in row.values()])
        console.print(table)


class ORCLI(cmd.Cmd):
    intro = 'Welcome to the orSQL shell. Type help or ? to list commands.\n'
    prompt = '(Or db) >> '

    def __init__(self, catalog, hf_token):
        super().__init__()
        self.catalog = catalog
        self.ai = AITranslator(hf_token, catalog) if hf_token else None
        self.executor = Executor(catalog)

    def do_hello(self, line):
        """Print a greeting."""
        print("Hello, World!")

    def do_get(self, arg):
        """Usage: get <table> <id> - Uses B-Tree O(log n) search"""
        parts = arg.split()
        if len(parts) == 1:
            # Legacy: assume 'users' table
            table_name, record_id = "users", parts[0]
        elif len(parts) == 2:
            table_name, record_id = parts
        else:
            print("Usage: get [table] <id>")
            return
        try:
            table = self.executor._get_table(table_name)
            res = table.select_by_id(int(record_id))
            if res:
                print(f"FOUND: {res}")
            else:
                print("Not found.")
        except Exception as e:
            print(f"Error: {e}")

    def do_quit(self, line):
        """Exit the CLI."""
        self.executor.close()
        return True

    def preloop(self):
        os.system('cls' if os.name == 'nt' else 'clear')

        tasks = [
            "Initializing B+ Tree...",
            "Mapping Data Pages...",
            "Loading Storage Engine...",
            "Ready."
        ]

        console.print("[bold cyan]System Check:[/bold cyan]")
        for task in track(tasks, description="[bold blue]Booting OR-SQL..."):
            time.sleep(0.4)

        ascii_art = r"""
    ____  ____        ____   ___  _
    / __ \|  _ \      / ___| / _ \| |
    | |  | | |_) |____ \___ \| | | | |
    | |__| |  _ <|____| ___) | |_| | |___
    \____/|_| \_\     |____/ \__\_\_____|
        """

        banner_content = Text(ascii_art, style="bold cyan")
        banner_content.append("\n" + "-" * 40, style="dim white")
        banner_content.append("\n[ THE FASTEST B-TREE ENGINE ]", style="italic yellow")

        banner = Panel(
            banner_content,
            title="[bold green] v2.0.0 [/bold green]",
            subtitle="[bold white]Press 'help' for commands[/bold white]",
            border_style="bright_blue",
            padding=(1, 2)
        )

        console.print(banner)

    def postloop(self):
        print("Goodbye!")

    def default(self, line: str):
        """Route SQL statements through the Executor."""
        try:
            result = self.executor.run(line, ai=self.ai)
            success_keywords = ["success", "inserted", "created", "deleted", "updated", "dropped"]

            if isinstance(result, list):
                if not result:
                    console.print("[yellow]Empty set (0 rows).[/yellow]")
                    return

                table = RichTable(show_header=True, header_style="bold cyan", border_style="bright_blue")
                for column in result[0].keys():
                    table.add_column(column.capitalize())
                for row in result:
                    table.add_row(*[str(val) for val in row.values()])

                console.print(table)
                console.print(f"[green]({len(result)} rows in set)[/green]")

            elif isinstance(result, str) and any(word in result.lower() for word in success_keywords):
                console.print(Panel(f"[bold green]{result}", border_style="green"))
            else:
                console.print(Panel(f"[bold red]Error:[/bold red] {result}", border_style="red"))

        except Exception as e:
            console.print(Panel(f"[bold white on red] CRITICAL ERROR [/bold white on red]\n{str(e)}", title="System"))

    def do_ai(self, line):
        """Translate natural language to SQL using AI."""
        if self.ai is None:
            print("AI translator not configured. Please provide a HuggingFace token.")
            print("Tip: set HF_TOKEN in .env to enable natural language queries.")
            return

        print("Translating...")
        sql = self.ai.translate(line)

        if sql is None:
            print("Sorry, I couldn't understand that request.")
            print("Try rephrasing, or type SQL directly.")
            return

        print(f"AI: {sql}")
        self.default(sql)

    def do_vacuum(self, arg):
        """Run VACUUM on a table. Usage: vacuum [table_name]"""
        table_name = arg.strip() or "users"
        try:
            table = self.executor._get_table(table_name)
            print(table.fragmentation_report())
            table.vacuum()
        except Exception as e:
            print(f"Error: {e}")

    def do_freelist(self, arg):
        """Show freelist status. Usage: freelist [table_name]"""
        table_name = arg.strip() or "users"
        try:
            table = self.executor._get_table(table_name)
            print(table.freelist_report())
        except Exception as e:
            print(f"Error: {e}")

    def do_cache(self, arg):
        """Show query cache statistics."""
        print(self.executor.cache.report())

    def do_report(self, arg):
        """Show fragmentation report. Usage: report [table_name]"""
        table_name = arg.strip() or "users"
        try:
            table = self.executor._get_table(table_name)
            print(table.fragmentation_report())
        except Exception as e:
            print(f"Error: {e}")


def get_db_dir():
    """Get database directory from env var or default."""
    path = os.environ.get("ORSQL_DB_DIR")
    if path:
        return path
    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_dir = os.path.join(base_dir, "db_files")
    os.makedirs(db_dir, exist_ok=True)
    return db_dir


if __name__ == '__main__':
    import sys

    load_dotenv()

    if len(sys.argv) > 1:
        db_dir = sys.argv[1]
    else:
        db_dir = get_db_dir()

    catalog = Catalog(db_dir)
    HF_TOKEN = os.environ.get("HF_TOKEN")

    if not HF_TOKEN:
        print("Warning: HF_TOKEN not set in .env file (AI features disabled)")

    cli = ORCLI(catalog, HF_TOKEN)
    cli.cmdloop()
