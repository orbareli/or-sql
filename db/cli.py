import cmd
from table import Table
from ast_sql.parser import Executor
import time , datetime
import sys
import cmd , os
import re
import ast
from aisql import AITranslator

from rich.console import Console
from rich.table import Table as RichTable
from rich.panel import Panel
from rich import box
# =========================
# Terminal UI Class
# =========================
from dotenv import load_dotenv
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
        console.print(f"[bold green]✔ {message}[/bold green]")

    @staticmethod
    def error(message):
        console.print(f"[bold red]✖ {message}[/bold red]")

    @staticmethod
    def info(message):
        console.print(f"[cyan]{message}[/cyan]")

    @staticmethod
    def print_table(rows):
        if not rows:
            console.print("[yellow](0 rows)[/yellow]")
            return

        table = RichTable(box=box.SIMPLE_HEAVY)
        
        # כותרות
        for key in rows[0].keys():
            table.add_column(str(key), style="green")

        # שורות
        for row in rows:
            table.add_row(*[str(v) for v in row.values()])

        console.print(table)

class ORCLI(cmd.Cmd):
    intro = 'Welcome to the orSQL shell. Type help or ? to list commands.\n'
    prompt = '(Or db) >> '
    def __init__(self, table_instance,hf_token):
        # חשוב מאוד: לקרוא ל-init של מחלקת האב (cmd.Cmd)
        super().__init__()
        # כאן אנחנו שומרים את המופע של ה-Table בתוך ה-CLI
        self.db = table_instance
        self.ai = AITranslator(hf_token)
        self.executor = Executor(table_instance)
    
    """def default(self, line: str):
        "Catch-all — every SQL statement comes through here."
        result = self.executor.run(line)
        print(result)"""
    

    def do_hello(self, line):
        """Print a greeting."""
        print("Hello, World!")
    """def default(self, line: str):
        result = self.executor.run(line,ai=self.ai)
        print(result)"""
    """def do_insert(self, arg):
        "Usage: insert <name> <age>"
        try:
        # arg מכיל את כל מה שכתבת אחרי המילה add (למשל "or 12")
            parts = arg.split()
            if len(parts) < 2:
                print("Error: You must provide both name and age.")
                return

            name = parts[0]
            age = int(parts[1])

            # שליחה למנוע - וודא שהסדר כאן תואם להגדרה ב-Table
            new_id = self.db.insert(name, age) 
            print(f"--> Record added with ID: {new_id}")

        except ValueError:
            print("Error: Age must be a number.")"""
    def do_SELECT(self, args):
        """Usage: get <id> - Uses B-Tree O(log n) search"""
        try:
            res = self.db.select_all()
            if res:

                for row in res:
                    print(f"FOUND: {row}")
            else:
                print("Not found.")
        except ValueError:
            print("Error: ID must be a number.")
            
    def do_get(self, arg):
        """Usage: get <id> - Uses B-Tree O(log n) search"""
        try:
            res = self.db.select_by_id(int(arg))
            if res:
                print(f"FOUND: {res}")
            else:
                print("Not found.")
        except ValueError:
            print("Error: ID must be a number.")

    def do_quit(self, line):
        """Exit the CLI."""
        return True
    def do_search(self,line):
        print("hiiiii")
    """def precmd(self, line):
    # Add custom code here
        print("Before command execution")
        return line  # You must return the modified or original command line
    def postcmd(self, stop, line):
    # Add custom code here
        print("After command execution")
        return stop  # Return 'stop' to control whether the CLI continues or exits"""
    def preloop(self):
            from rich.console import Console
            from rich.panel import Panel
            from rich.progress import track
            from rich.text import Text
            import os
            import time

            console = Console()

            # 1. ניקוי מסך לחוויה נקייה
            os.system('cls' if os.name == 'nt' else 'clear')

            # 2. האנימציה של הטעינה (בסגנון מקצועי)
            tasks = [
                "Initializing B+ Tree...",
                "Mapping Data Pages...",
                "Loading Storage Engine...",
                "Ready."
            ]
            
            console.print("[bold cyan]System Check:[/bold cyan]")
            for task in track(tasks, description="[bold blue]Booting OR-SQL..."):
                time.sleep(0.4)

            # 3. הלוגו החדש והישר (Block Style)
            # הלוגו הזה בנוי מתווים פשוטים שלא נשברים בטרמינל
            ascii_art = r"""
    ____  ____        ____   ___  _     
    / __ \|  _ \      / ___| / _ \| |    
    | |  | | |_) |____ \___ \| | | | |    
    | |__| |  _ <|____| ___) | |_| | |___ 
    \____/|_| \_\     |____/ \__\_\_____|
            """
            
            banner_content = Text(ascii_art, style="bold cyan")
            banner_content.append("\n" + "─" * 40, style="dim white")
            banner_content.append("\n[ THE FASTEST B-TREE ENGINE ]", style="italic yellow")
            
            # 4. יצירת ה-Panel
            banner = Panel(
                banner_content,
                title="[bold green] v1.0.0 [/bold green]",
                subtitle="[bold white]Press 'help' for commands[/bold white]",
                border_style="bright_blue",
                padding=(1, 2)
            )
            
            console.print(banner)
    def postloop(self):
    # Add custom cleanup or finalization here
        print("Finalization after the CLI loop")
    def default(self, line: str):
        
        try:
            # הרצת השאילתה דרך ה-Executor
            result = self.executor.run(line, ai=self.ai)
            success_keywords = ["success", "inserted", "created", "deleted", "updated"]
            # 1. אם התוצאה היא רשימה (כמו ב-SELECT)
            if isinstance(result, list):
                if not result:
                    console.print("[yellow]Empty set (0 rows).[/yellow]")
                    return

                # יצירת טבלה מעוצבת
                table = RichTable(show_header=True, header_style="bold cyan", border_style="bright_blue")
                
                # הוספת עמודות לפי המפתחות במילון הראשון
                for column in result[0].keys():
                    table.add_column(column.capitalize())

                # הוספת השורות לטבלה
                for row in result:
                    table.add_row(*[str(val) for val in row.values()])

                console.print(table)
                console.print(f"[green]({len(result)} rows in set)[/green]")

            # 2. אם התוצאה היא הודעת הצלחה (כמו ב-INSERT/DELETE)
            elif isinstance(result, str) and any(word in result.lower() for word in success_keywords):
                console.print(Panel(f"[bold green]✓[/bold green] {result}", border_style="green"))
            # 3. אם זו הודעת שגיאה
            else:
                console.print(Panel(f"[bold red]Error:[/bold red] {result}", border_style="red"))

        except Exception as e:
            print(e)
            console.print(Panel(f"[bold white on red] CRITICAL ERROR [/bold white on red]\n{str(e)}", title="System"))
    def do_ai(self,line):
            # Looks like English — ask AI
        if self.ai is None:
            print("AI translator not configured. Please provide a HuggingFace token.")
            print("Tip: start OrSQL with a token to enable natural language queries.")
            return

        print("Translating...")
        sql = self.ai.translate(line)

        if sql is None:
            print("Sorry, I couldn't understand that request.")
            print("Try rephrasing, or type SQL directly.")
            return

        # Show the user what SQL was generated
        print(f"AI: {sql}")

                # Run the generated SQL
        self.default(sql)
        #result = self.executor.run(sql)
        #print(result)
    def do_vacuum(self,arg):
        print(self.executor.table.fragmentation_report())
        self.executor.table.vacuum()
        return
    def do_freelist(self, arg):
        """Show freelist status."""
        print(self.executor.table.freelist_report())
    def do_cache(self, arg):
        print(self.executor.cache.report())
        return
    def do_report(self,arg):
        print(self.executor.table.fragmentation_report())
        return

if __name__ == '__main__':
    my_table = Table(r"C:\Users\or\Desktop\or-sql\db\db_files\my_database2.db")
    #cli = MyCLI(my_table)
    load_dotenv()  # reads .env file automatically
    HF_TOKEN = os.environ.get("HF_TOKEN")

    if not HF_TOKEN:
        print("Warning: HF_TOKEN not set in .env file")
    cli = ORCLI(my_table,HF_TOKEN)
    cli.cmdloop()
