import re


def clean_formatting(text: str) -> str:
    """
    Cleans up common punctuation spacing issues and bullet formatting:
    - Fixes excessive spaces after periods or commas.
    - Converts '. -' into '.\n- ' to properly separate bullet lines.
    - Normalizes spaced double periods '. .' into '. '.
    """
    # Matches periods or commas followed by 2+ spaces
    spaced_period_pattern = re.compile(r"[.,]\s{2,}")
    text = spaced_period_pattern.sub(". ", text)
    
    # Matches '. -' pattern to start a bullet properly
    period_into_dash_pattern = re.compile(r"\.\s\-")
    text = period_into_dash_pattern.sub(".\n- ", text)
    
    # Matches spaced double periods like '. .' into '. '
    spaced_double_period_pattern = re.compile(r"\.\s\.")
    text = spaced_double_period_pattern.sub(". ", text)
    
    return text


def normalize_legal_text(text: str) -> str:
    # Split the input text into individual lines
    lines = text.splitlines()
    # Store the processed lines
    normalized_lines = []
    # Track if the previous line was a numbered list item
    prev_was_numbered = False

    for i, line in enumerate(lines):
        # Remove leading/trailing spaces for checks
        stripped = line.strip()

        # 1️⃣ Handle completely empty lines
        if not stripped:
            # If last line was numbered, skip adding blank line (keep numbered items tight)
            if prev_was_numbered:
                continue
            else:
                # Otherwise keep the blank line
                normalized_lines.append("")
                prev_was_numbered = False
                continue

        # 2️⃣ Check if the line is a numbered item like '1.' or '2.'
        number_match = re.match(r'^(\d+\.)', stripped)
        if number_match:
            # Add the line as-is (no additional indentation)
            normalized_lines.append(stripped)
            # Mark that we just added a numbered item
            prev_was_numbered = True
            continue

        # 3️⃣ Check for bullet lines that start with spaces + dash
        bullet_match = re.match(r'^( +\- )(.*)', line)
        if bullet_match:
            # Extract the spaces before the dash (like ' - ' or '    - ')
            spaces_before_dash = bullet_match.group(1)
            # Extract the text content after the dash
            content = bullet_match.group(2)
            # Determine nesting level based on spaces before dash (each 2 spaces = level)
            nesting_level = len(spaces_before_dash) // 2
            # Compute extra indentation for deeper nesting
            indent = '  ' * (nesting_level - 1) if nesting_level > 1 else ''
            # Build the normalized bullet line
            normalized_lines.append(f"{indent}{spaces_before_dash}{content}")
            prev_was_numbered = False
            continue

        # 4️⃣ Otherwise, treat as normal line (like headings or paragraph text)
        # Preserve original line trimming trailing spaces
        normalized_lines.append(line.rstrip())
        prev_was_numbered = False

    # Finally join all processed lines into a single string with newlines
    return "\n".join(normalized_lines)
    final_output = []
    for i, line in enumerate(normalized_lines):
        if i > 0:
            prev_line = normalized_lines[i-1]
            # Check if both current and previous lines are numbered
            if re.match(r'^\d+\.', prev_line) and re.match(r'^\d+\.', line):
                # do NOT add an extra newline
                final_output.append(line)
                continue
        final_output.append('\n' + line)
    
    # The very first line doesn't need a leading \n
    final_text = ''.join(final_output).lstrip('\n')


import re

def normalize_legal_text(text: str) -> str:
    """
    Processes legal or structured text to normalize numbered lists,
    bullet sub-lists, and headings, ensuring:
    - No blank lines between list items or sub-lists.
    - Proper indentation based on sub-list nesting.
    - Headings and normal text maintain line breaks appropriately.
    """
    # Split text into individual lines
    lines = text.splitlines()
    
    # Will accumulate the final formatted text
    normalized_text = ""
    
    # Tracks what kind of line we processed last: 'number', 'bullet', or 'normal'
    last_line_type = None

    for line in lines:
        # Strip spaces for matching
        stripped = line.strip()
        
        # Skip purely empty lines entirely (do not add to output)
        if not stripped:
            continue

        # 1️⃣ Check for numbered list items like '1.', '2.', etc
        if re.match(r'^\d+\.', stripped):
            if last_line_type in {'number', 'bullet'}:
                # If previous was also a list item, add directly with no extra newline
                normalized_text += f"{stripped}"
            else:
                # If first item or after normal text, add newline
                if normalized_text:
                    normalized_text += f"\n{stripped}"
                else:
                    normalized_text = stripped
            last_line_type = 'number'
            continue

        # 2️⃣ Check for bullet sub-lists like ' - ...'
        bullet_match = re.match(r'^( +\- )(.*)', line)
        if bullet_match:
            spaces_before_dash = bullet_match.group(1)  # e.g. ' - '
            content = bullet_match.group(2)            # text after dash
            # Calculate nesting level (each 2 spaces assumed as one level)
            nesting_level = len(spaces_before_dash) // 2
            # Add further indentation if nested
            indent = '  ' * (nesting_level - 1) if nesting_level > 1 else ''
            # Build bullet line with appropriate indentation
            bullet_line = f"{indent}{spaces_before_dash}{content}"
            
            # Always ensure bullet lines start on their own line
            normalized_text += f"\n{bullet_line}"
            last_line_type = 'bullet'
            continue

        # 3️⃣ For normal lines (headings or paragraph text)
        # Insert newline before if needed to keep separation
        if normalized_text:
            normalized_text += f"\n{stripped}"
        else:
            normalized_text = stripped
        last_line_type = 'normal'

    return normalized_text


import re

def normalize_legal_text(text: str) -> str:
    """
    Processes legal or structured text to normalize:
    - Numbered lists
    - Bullet sub-lists with ' -'
    - Headings and paragraphs
    Ensures:
    - No blank lines between list items or sub-lists.
    - Proper indentation for sub-lists.
    - Clean one-line separation of items.
    """
    lines = text.splitlines()
    normalized_lines = []
    last_line_type = None  # Tracks: 'number', 'bullet', 'normal'

    for line in lines:
        stripped = line.strip()

        # Skip purely empty lines
        if not stripped:
            continue

        # 1️⃣ Check for numbered items like '1.', '2.'
        if re.match(r'^\d+\.', stripped):
            normalized_lines.append(stripped)
            last_line_type = 'number'
            continue

        # 2️⃣ Check for bullets like ' - ...'
        bullet_match = re.match(r'^( +\- )(.*)', line)
        if bullet_match:
            spaces_before_dash = bullet_match.group(1)
            content = bullet_match.group(2)
            nesting_level = len(spaces_before_dash) // 2
            indent = '  ' * (nesting_level - 1) if nesting_level > 1 else ''
            bullet_line = f"{indent}{spaces_before_dash}{content}"
            normalized_lines.append(bullet_line)
            last_line_type = 'bullet'
            continue

        # 3️⃣ Otherwise treat as normal heading or paragraph line
        normalized_lines.append(line.rstrip())
        last_line_type = 'normal'

    # Join all lines with a single \n between each
    return "\n".join(normalized_lines)


import re

def normalize_legal_text(text: str) -> str:
    lines = text.splitlines()
    normalized_lines = []
    inside_list_block = False
    last_list_indent_level = 0

    for line in lines:
        stripped = line.rstrip()

        # Blank line
        if not stripped.strip():
            if inside_list_block:
                # Skip blank lines inside a list block
                continue
            else:
                # Keep blank lines outside list block
                normalized_lines.append("")
                continue

        # Numbered list item (like 1. something)
        if re.match(r'^\d+\.', stripped):
            normalized_lines.append(stripped)
            inside_list_block = True
            last_list_indent_level = 0  # reset on new numbered item
            continue

        # Bullet item (like "    - item")
        bullet_match = re.match(r'^(\s*)-\s+(.*)', line)
        if bullet_match:
            spaces = bullet_match.group(1)
            content = bullet_match.group(2)
            nesting_level = len(spaces) // 2
            indent = '  ' * max(nesting_level, 0)
            normalized_lines.append(f"{indent}- {content}")
            inside_list_block = True
            last_list_indent_level = nesting_level
            continue

        # If we are inside a list block but this line is not a bullet,
        # treat it as continuation or explanation under list
        if inside_list_block:
            indent = '  ' * last_list_indent_level
            normalized_lines.append(f"{indent}{stripped}")
            continue

        # Otherwise normal line outside any list
        normalized_lines.append(stripped)
        inside_list_block = False

    return "\n".join(normalized_lines)


import re

def normalize_legal_text(text: str) -> str:
    lines = text.splitlines()
    normalized_lines = []
    inside_list_block = False
    last_list_indent_level = 0
    last_numbered_value = 0

    for line in lines:
        stripped = line.rstrip()

        # Blank line
        if not stripped.strip():
            if inside_list_block:
                # Skip blank lines inside a list block
                continue
            else:
                # Keep blank lines outside list block
                normalized_lines.append("")
                continue

        # Numbered list item (like "1. text")
        num_match = re.match(r'^(\d+)\.', stripped)
        if num_match:
            current_number = int(num_match.group(1))
            # If we see a "1." immediately after a bigger number (like after 5.), force separation
            if current_number == 1 and last_numbered_value > 1:
                normalized_lines.append("")  # insert blank line to start new logical section
            normalized_lines.append(stripped)
            inside_list_block = True
            last_list_indent_level = 0
            last_numbered_value = current_number
            continue

        # Bullet item (like "    - item")
        bullet_match = re.match(r'^(\s*)-\s+(.*)', line)
        if bullet_match:
            spaces = bullet_match.group(1)
            content = bullet_match.group(2)
            nesting_level = len(spaces) // 2
            indent = '  ' * max(nesting_level, 0)
            normalized_lines.append(f"{indent}- {content}")
            inside_list_block = True
            last_list_indent_level = nesting_level
            continue

        # Inside list block continuation line (not bullet)
        if inside_list_block:
            indent = '  ' * last_list_indent_level
            normalized_lines.append(f"{indent}{stripped}")
            continue

        # Normal outside line
        normalized_lines.append(stripped)
        inside_list_block = False
        last_numbered_value = 0  # reset numbering context

    return "\n".join(normalized_lines)



import re

def normalize_legal_text(text: str) -> str:
    lines = text.splitlines()
    normalized_lines = []
    inside_list_block = False
    last_list_indent_level = 0
    last_numbered_value = 0
    after_number_line = False

    for i, line in enumerate(lines):
        stripped = line.rstrip()

        # Handle blank lines smartly
        if not stripped.strip():
            if inside_list_block and not after_number_line:
                # inside a list block - skip blank line
                continue
            else:
                normalized_lines.append("")
                continue

        # Numbered line (like 1.)
        num_match = re.match(r'^(\d+)\.', stripped)
        if num_match:
            current_number = int(num_match.group(1))
            if current_number == 1 and last_numbered_value > 1:
                # e.g. after point 5. comes 1. again => new section
                normalized_lines.append("")
            normalized_lines.append(stripped)
            inside_list_block = True
            last_list_indent_level = 0
            last_numbered_value = current_number

            # Now peek ahead
            if i+1 < len(lines):
                next_line = lines[i+1]
                if next_line.startswith(" "):
                    after_number_line = True
                else:
                    after_number_line = False
            else:
                after_number_line = False
            continue

        # Bullet line
        bullet_match = re.match(r'^(\s*)-\s+(.*)', line)
        if bullet_match:
            spaces = bullet_match.group(1)
            content = bullet_match.group(2)
            nesting_level = len(spaces) // 2
            indent = '  ' * max(nesting_level, 0)
            normalized_lines.append(f"{indent}- {content}")
            inside_list_block = True
            last_list_indent_level = nesting_level
            after_number_line = False
            continue

        # If directly after a numbered line, decide based on indentation
        if after_number_line:
            if line.startswith(" "):
                indent = '  ' * last_list_indent_level
                normalized_lines.append(f"{indent}{stripped}")
                inside_list_block = True
            else:
                # Not indented, break list block
                normalized_lines.append("")
                normalized_lines.append(stripped)
                inside_list_block = False
            after_number_line = False
            continue

        # Inside list block
        if inside_list_block:
            indent = '  ' * last_list_indent_level
            normalized_lines.append(f"{indent}{stripped}")
            continue

        # Normal line outside any list
        normalized_lines.append(stripped)
        inside_list_block = False
        last_numbered_value = 0

    return "\n".join(normalized_lines)


import re

def normalize_legal_text(text: str) -> str:
    lines = text.splitlines()
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
        bullet_match = re.match(r'^(\s*)-\s+(.*)', line)
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

    return "\n".join(normalized_lines)





