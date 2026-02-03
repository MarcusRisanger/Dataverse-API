# Upsert Match Parameter Example

The `match` parameter in the `upsert` method allows you to control whether the operation should only create new records or only update existing ones.

## Usage

### Standard Upsert (Default Behavior)
```python
# Creates new records or updates existing ones
entity.upsert(data, mode="individual")
```

### Prevent Create (Only Update)
```python
# Only updates existing records, will fail if record doesn't exist
# Uses If-Match: * header
entity.upsert(data, mode="individual", match="prevent_create")
```

### Prevent Update (Only Create)
```python
# Only creates new records, will fail if record already exists
# Uses If-None-Match: * header
entity.upsert(data, mode="individual", match="prevent_update")
```

## Batch Mode Support
The `match` parameter works with both individual and batch modes:

```python
# Batch mode with prevent_create
entity.upsert(data, mode="batch", match="prevent_create")

# Batch mode with prevent_update
entity.upsert(data, mode="batch", match="prevent_update")
```

## Reference
For more details on the underlying Dataverse Web API behavior, see:
https://learn.microsoft.com/en-us/power-apps/developer/data-platform/use-upsert-insert-update-record
