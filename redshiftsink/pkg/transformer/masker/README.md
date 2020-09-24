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

### NonPii Keys
Mask all the columns in all the tables in `inventory` database except the column `id` in `customers` table.

**/usr/inventory.yaml**
```yaml
non_pii_keys:
    customers:
    - id
```

### Length Keys
Creates extra column containing the length or original column. `email_length` gets created containing the lenght of data in `email` column.

```yaml
length_keys:
    customers:
    - email
```

### Sort Keys
Specify one or more columns in a table as Redshift Sort Key.

```yaml
sort_keys:
    customers:
    - created_at
```

### Dist Keys
Specify one or more columns in a table as Redshift Disk Key.

```yaml
dist_keys:
    customers:
    - account_id
```    
