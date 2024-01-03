from upstash_qstash.qstash_types import UpstashHeaders


def prefix_headers(headers: UpstashHeaders):
    """
    Destructively prefixes certain headers with 'Upstash-Forward-'.

    :param headers: A dictionary representing the headers of the HTTP request to be delivered.
                    Headers starting with 'content-type' or 'upstash-' are ignored and not prefixed.
    """

    def is_ignored_header(header):
        lower_case_header = header.lower()
        return lower_case_header.startswith(
            "content-type"
        ) or lower_case_header.startswith("upstash-")

    keys_to_be_prefixed = [key for key in headers.keys() if not is_ignored_header(key)]

    for key in keys_to_be_prefixed:
        value = headers.get(key)
        if value is not None:
            headers[f"Upstash-Forward-{key}"] = value
        del headers[key]
