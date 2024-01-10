from upstash_qstash.qstash_types import UpstashHeaders


def prefix_headers(headers: dict) -> UpstashHeaders:
    """
    Prefixes certain headers with 'Upstash-Forward-'.

    :param headers: A dictionary representing the headers of the HTTP request to be delivered.
                    Headers starting with 'content-type' or 'upstash-' are ignored and not prefixed.
    :return: A dictionary representing the prefixed headers.
    """

    def is_ignored_header(header):
        lower_case_header = header.lower()
        return lower_case_header.startswith(
            "content-type"
        ) or lower_case_header.startswith("upstash-")

    new_headers = {}
    for key, value in headers.items():
        if is_ignored_header(key):
            new_headers[key] = value
        else:
            new_headers[f"Upstash-Forward-{key}"] = value

    return new_headers
