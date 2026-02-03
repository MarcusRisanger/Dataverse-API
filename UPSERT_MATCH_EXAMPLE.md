# Upsert Match Parameter Example

The `match` parameter in the `upsert` method allows you to control whether the operation should only create new records or only update existing ones.

**Note:** The `match` parameter is only supported for **individual mode**, not batch mode.

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
# Only works with mode="individual"
entity.upsert(data, mode="individual", match="prevent_create")
```

### Prevent Update (Only Create)
```python
# Only creates new records, will fail if record already exists
# Uses If-None-Match: * header
# Only works with mode="individual"
entity.upsert(data, mode="individual", match="prevent_update")
```

## Batch Mode
The `match` parameter is **not supported** for batch mode operations. Attempting to use it will raise a `DataverseError`:

```python
# This will raise an error
entity.upsert(data, mode="batch", match="prevent_create")  # Error!
```

For batch operations, use standard upsert behavior without the `match` parameter:

```python
# Standard batch upsert (create or update)
entity.upsert(data, mode="batch")
```

## Reference
For more details on the underlying Dataverse Web API behavior, see:
https://learn.microsoft.com/en-us/power-apps/developer/data-platform/use-upsert-insert-update-record
