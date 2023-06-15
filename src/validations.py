from typing import Any, Dict, List


def key_is_in_entrypoint_arguments(key: str, arguments: List[Dict[str, Any]]) -> bool:
    """Checks if a key is in the entrypoint arguments."""
    for arg in arguments:
        if arg['name'] == key:
            return True
    return False


def schedule_request_is_valid(request_data: Dict[str, Any]) -> bool:
    """Checks that all the parameters to schedule a task to EMR is valid."""
    return request_data is not None and \
        "name" in request_data and \
        "algorithm" in request_data and \
        "entrypoint_arguments" in request_data and \
        key_is_in_entrypoint_arguments("app-name", request_data['entrypoint_arguments'])
