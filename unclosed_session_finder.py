import ast
import sys

class SessionUsageAnalyzer(ast.NodeVisitor):
    def __init__(self):
        self.unclosed_sessions = []  # (line, name)
        self.current_function = None

    def visit_FunctionDef(self, node):
        self.current_function = node
        self.generic_visit(node)
        self.current_function = None

    def visit_Assign(self, node):
        if isinstance(node.value, ast.Call):
            if isinstance(node.value.func, ast.Name) and node.value.func.id == "Session":
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        session_name = target.id
                        if not self.session_closed_properly(self.current_function, session_name):
                            self.unclosed_sessions.append((node.lineno, session_name))
        self.generic_visit(node)

    def session_closed_properly(self, func_node, session_name):
        if func_node is None:
            return False  # global sessions: suspicious

        closed_lines = set()
        return_or_raise_lines = set()
        closed_in_finally = False

        for subnode in ast.walk(func_node):
            # Check for session.close() calls
            if isinstance(subnode, ast.Expr):
                if isinstance(subnode.value, ast.Call):
                    func = subnode.value.func
                    if isinstance(func, ast.Attribute):
                        if func.attr == "close" and isinstance(func.value, ast.Name):
                            if func.value.id == session_name:
                                closed_lines.add(subnode.lineno)

            # Collect return and raise lines
            elif isinstance(subnode, (ast.Return, ast.Raise)):
                return_or_raise_lines.add(subnode.lineno)

            # Check if close is in a finally block
            elif isinstance(subnode, ast.Try):
                for final_node in subnode.finalbody:
                    for final_sub in ast.walk(final_node):
                        if isinstance(final_sub, ast.Expr):
                            if isinstance(final_sub.value, ast.Call):
                                func = final_sub.value.func
                                if isinstance(func, ast.Attribute):
                                    if func.attr == "close" and isinstance(func.value, ast.Name):
                                        if func.value.id == session_name:
                                            closed_in_finally = True

        if closed_in_finally:
            return True  # always considered safe

        if not return_or_raise_lines:
            return bool(closed_lines)

        for ret_line in return_or_raise_lines:
            closes_before = [l for l in closed_lines if l < ret_line]
            if not closes_before:
                return False
        return True


def analyze_file(filename):
    with open(filename, "r", encoding="utf-8") as f:
        source = f.read()

    try:
        tree = ast.parse(source, filename=filename)
    except SyntaxError as e:
        print(f"Syntax error in {filename}: {e}")
        return

    analyzer = SessionUsageAnalyzer()
    analyzer.visit(tree)

    if not analyzer.unclosed_sessions:
        print("✅ Alle Session-Instanzen wurden korrekt geschlossen.")
    else:
        print("❌ Unclosed sessions gefunden:")
        for lineno, name in analyzer.unclosed_sessions:
            print(f"  - Session '{name}' geöffnet in Zeile {lineno}, aber nicht vor allen return/raise geschlossen.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python check_sessions.py <filename.py>")
        sys.exit(1)

    analyze_file(sys.argv[1])
