## Masking

Masking when enabled masks all columns by default unless specified not to mask.

## Configuration
Specification of what to unmask is specified using a configuration file.
- Masking needs to be enabled in redshiftbatcher configuration. (to mask column values)
- Masking needs to be enabled in redshiftloader configuration. (to choose correct column types)

### Convention:
There is a convention for the mask configuration file name:
```
If,     mask=true            (in redshiftbatcher config)
        maskConfigDir="/usr" (in redshiftbatcher config)
        topic="datapipe.inventory.customers"
Then,
        configuration file should be present at: /usr/inventory.yaml
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

### Conditional NonPiiKeys
Conditional NonPiiKeys unmasks columns if it matches any of the pattern in the pattern list.
```yaml
conditional_non_pii_keys:
    customers:
        email:
        - '%example.com'
        - '%exampledev.com'
```

### Dependent NonPiiKeys
Dependent NonPiiKeys unmask a column based on the values of other columns.
```yaml
dependent_non_pii_keys:
    customers:
        # dependentColumnName
        first_name:
            # providerColumn
            last_name:
            - 'Jones'
            - 'Dhoni'
```

### Length Keys
Creates extra column containing the length or original column. `email_length` gets created containing the length of data in `email` column.

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
