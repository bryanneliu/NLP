import collections
import re

def extract_ranges(token_list):
    ranges = collections.defaultdict(list)
    last_start = None
    current_type = None
    for token in token_list:
        if "/" not in token:
            continue
        word, tag = token.split("/")
        interval_type = tag.rpartition("_")[-1] or "START_END"
        range_type = tag.rpartition("_")[0]

        if interval_type not in {"START", "END", "START_END"}:
            interval_type = "START_END"
            range_type = tag

        components = list(filter(None, re.split(r"(\d+)", word)))
        # components should either contain a single number or a number followed
        # by a letter.
        interval_value = int(components[0])
        if len(components) == 2:
            range_type = f"{components[1]}.{range_type}"

        if interval_type == "END":
            ranges[range_type].append((last_start, interval_value))
            last_start = None
            current_type = None
        else:
            # Finish existing interval, if needed.
            if last_start:
                # Must be an unbounded interval, e.g. girls 1+
                ranges[current_type].append((last_start, float('inf')))
                last_start = None
                current_type = None
            if interval_type == "START":
                last_start = interval_value
                current_type = range_type
            else:
                # Must be a single value, e.g. 1 year old girls
                ranges[range_type].append((interval_value, interval_value))
    # Finish existing interval, if needed.
    if last_start:
        # Must be an unbounded interval, e.g. girls 1+
        ranges[current_type].append((last_start, float('inf')))
    return ranges


# Support full match for non-measurement tokens
# Support range match for the same measurement:
#   query and expression ranges have overlap for the same measurement
def is_range_full_match(expression, query):
    if not expression or not query:
        return 0

    query_ranges = extract_ranges(query.split(" "))
    subset_token_list = expression.split(" ")
    expression_ranges = extract_ranges(subset_token_list)
    for range_type, expression_range_values in expression_ranges.items():
        if range_type not in query_ranges:
            return 0
        for expression_range_value in expression_range_values:
            # Check if there is a range in the query that overlaps with this one.
            found_match = False
            for query_range_value in query_ranges[range_type]:
                exp_start, exp_end = expression_range_value
                query_start, query_end = query_range_value
                if query_end < exp_start or exp_end < query_start:
                    continue
                found_match = True
                break
            if not found_match:
                return 0

    superset_token_set = set(query.split(" "))
    for token in subset_token_list:
        if token in superset_token_set:
            continue

        if "/" in token:
            # Range values already checked
            continue

        return 0

    return 1


print(extract_ranges(["1/AGE_YEAR_START", "3/AGE_YEAR_END"]))
print(extract_ranges(["3t/SIZE_START", "4t/SIZE_END"]))
print(extract_ranges(["1/AGE_YEAR_START"]))
print(extract_ranges(["1/AGE_YEAR_START", "2t/SIZE_START", "3t/SIZE_END"]))
print(extract_ranges(["1/AGE_YEAR"]))

query = "3/AGE_YEAR_START 5/AGE_YEAR_END toddler toy"
expression = "toddler toy 4/AGE_YEAR_START"
print(f"1 == {is_range_full_match(expression, query)}")
query = "3/AGE_YEAR_START 5/AGE_YEAR_END toddler toy"
expression = "toddler toy 6/AGE_YEAR_START"
print(f"0 == {is_range_full_match(expression, query)}")
query = "3/AGE_YEAR_START 5/AGE_YEAR_END toddler toy"
expression = "toddler toy 1/AGE_YEAR_START"
print(f"1 == {is_range_full_match(expression, query)}")
query = "3/AGE_YEAR toddler toy"
expression = "toddler toy 2/AGE_YEAR_START 4/AGE_YEAR_END"
print(f"1 == {is_range_full_match(expression, query)}")
query = "3/AGE_YEAR toddler toy"
expression = "toddler toy 4/AGE_YEAR_START 14/AGE_YEAR_END"
print(f"0 == {is_range_full_match(expression, query)}")
query = "3/AGE_YEAR toddler toy"
expression = "kid toy 2/AGE_YEAR_START 4/AGE_YEAR_END"
print(f"0 == {is_range_full_match(expression, query)}")

query = "3t/SIZE_START 5t/SIZE_END toddler girl"
expression = "toddler girl 7t/SIZE_START 8t/SIZE_END"
print(f"0 == {is_range_full_match(expression, query)}")
query = "3t/SIZE_START 5t/SIZE_END toddler girl"
expression = "toddler girl 2t/SIZE_START 4t/SIZE_END"
print(f"1 == {is_range_full_match(expression, query)}")
query = "3t/SIZE_START 5t/SIZE_START toddler girl"
expression = "toddler girl 2x/SIZE_START 4x/SIZE_END"
print(f"0 == {is_range_full_match(expression, query)}")