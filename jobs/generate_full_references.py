import json
from waba.full_references import publish_full_references

if __name__ == "__main__":
    print(json.dumps(publish_full_references()), flush=True)
