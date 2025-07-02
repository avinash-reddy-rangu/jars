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

