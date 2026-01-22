import os
import yaml


def count_unique_models_in_selectors(yaml_file_path: str) -> str:
    model_usage = {}

    with open(yaml_file_path, "r") as file:
        data = yaml.safe_load(file)

    for selector in data.get("selectors", []):
        if "definition" in selector and "union" in selector["definition"]:
            ignore_selector = False

            for model in selector["definition"]["union"]:
                if "intersection" in model:
                    for item in model["intersection"]:
                        if (
                            item.get("method") == "source_status"
                            and item.get("value") == "fresher"
                        ):
                            ignore_selector = True
                            break
                elif (
                    "value" in model
                    and model.get("method") == "source_status"
                    and model.get("value") == "fresher"
                ):
                    ignore_selector = True
                    break

            if ignore_selector:
                continue

            for model in selector["definition"]["union"]:
                if "value" in model:
                    model_value = model["value"]
                elif "intersection" in model:
                    for item in model["intersection"]:
                        if "value" in item:
                            model_value = item["value"]
                        else:
                            continue
                else:
                    continue

                if model_value in model_usage:
                    model_usage[model_value].append(selector["name"])
                else:
                    model_usage[model_value] = [selector["name"]]

    total_unique_models = len(model_usage)
    duplicates = {
        model: selectors
        for model, selectors in model_usage.items()
        if len(selectors) > 1
    }

    if duplicates:
        error_message = "Error: Models used in more than one selector:\n"
        for model, selectors in duplicates.items():
            error_message += (
                f"Model '{model}' is used in selectors: {', '.join(selectors)}\n"
            )
        raise ValueError(error_message)

    return f"Total unique models in selectors: {total_unique_models}"


base_dir = os.path.dirname(os.path.abspath(__file__))
yaml_file_path = os.path.join(base_dir, "selectors.yml")

try:
    result = count_unique_models_in_selectors(yaml_file_path)
    print(result)
except ValueError as e:
    print(e)