import re

def format_text(text: str) -> str:
    try:
        text = re.sub(r'\n\.\n\s*(-)', r'\n -', text)
        lines = text.splitlines()
        break_flag = False
        for i, line in enumerate(lines):
            if line.strip() == "":
                continue
            if line.strip().lower() == "plaintext" or line.strip().lower() == "plain text":
                lines.pop(i)
            if len(line.strip()) > 0:
                break_flag = True
            if break_flag:
                break  # Break regardless after first non-empty line
        text = "\n".join(lines)
        normalized_lines = []
        inside_list_block = False
        last_list_indent_level = 0
        last_numbered_value = 0

        for line in lines:
            stripped = line.rstrip()

            # Blank line
            if not stripped.strip():
                if inside_list_block:
                    # skip blank lines inside list block
                    continue
                else:
                    normalized_lines.append("")
                    continue

            # Numbered item
            num_match = re.match(r'^(\d+)\.', stripped)
            if num_match:
                current_number = int(num_match.group(1))
                if current_number == 1 and last_numbered_value > 1:
                    normalized_lines.append("")  # separate multiple lists
                normalized_lines.append(stripped)
                inside_list_block = True
                last_list_indent_level = 0
                last_numbered_value = current_number
                continue

            # Bullet item
            bullet_match = re.match(r'^( +\- )(.*)', line)
            if bullet_match:
                spaces = bullet_match.group(1)
                content = bullet_match.group(2)
                nesting_level = len(spaces) // 2
                indent = '  ' * max(nesting_level, 0)
                normalized_lines.append(f"{indent}- {content}")
                inside_list_block = True
                last_list_indent_level = nesting_level
                continue

            # Indented explanation under list block
            if inside_list_block and line.startswith(" "):
                indent = '  ' * last_list_indent_level
                normalized_lines.append(f"{indent}{stripped}")
                continue

            # If we were inside a list block but now see normal text,
            # we add a blank line to clearly separate it.
            if inside_list_block:
                normalized_lines.append("")
                inside_list_block = False
                last_numbered_value = 0

            # Normal line
            normalized_lines.append(stripped)

        # --------- 🆕 Fix indentation for clause levels like 1.1, 1.1.a, 1.1.a.i ----------
        final_lines = []
        clause_pattern = re.compile(r'^((\d+)(\.\d+)?(\.[a-z])?(\.[ivx]+)?)(\s+)(.*)', re.IGNORECASE)

        for line in normalized_lines:
            match = clause_pattern.match(line)
            if match:
                clause = match.group(1)
                content = match.group(7)
                levels = clause.count(".")
                indent = "  " * levels  # 2 spaces per level
                final_lines.append(f"{indent}{clause} {content}")
            else:
                final_lines.append(line)

        return "\n".join(final_lines)
    except Exception as e:
        return text
