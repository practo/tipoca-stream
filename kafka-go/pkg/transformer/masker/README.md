## Masking

Masking when enabled masks all columns in all tables unless specified not to.

## Configuration

### Prequisite:
- Masking needs to be enabled in redshiftbatcher configuration. (to mask column values)
- Masking needs to be enabled in redshiftloader configuration. (to choose correct column types)
```
If,
    mask=true            (in redshiftbatcher config)
    maskConfigDir="/usr" (in redshiftbatcher config)
    topic="datapipe.inventory.customers"
Then, configuration file should be present at:
    /usr/inventory.yaml
```

## Features

### Non Pii Masking

Mask all the columns in all the tables in `inventory` database except the column `id` in `customers` table.

**/usr/inventory.yaml**
```yaml
non_pii_keys:
    customers:
    - id
```
