import random

def generate_pairs(total_lines=5000, min_val=10, max_val=99, repeat_ratio=0.05):
    unique_lines = int(total_lines * (1 - repeat_ratio))
    repeated_lines = total_lines - unique_lines

    # Generate unique lines
    lines = set()
    while len(lines) < unique_lines:
        a, b = random.randint(min_val, max_val), random.randint(min_val, max_val)
        lines.add(f"{a} {b}")

    lines = list(lines)

    # Add a few repeated lines
    for _ in range(repeated_lines):
        lines.append(random.choice(lines))

    # Shuffle the final list
    random.shuffle(lines)

    # Write to output.txt
    with open("output.txt", "w") as f:
        for line in lines:
            f.write(line + "\n")

generate_pairs()