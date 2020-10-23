## Masking

Masking when enabled masks all columns by default unless specified not to mask.

## Configuration
Specification of what to unmask is specified using a configuration file. Masking needs to be enabled in redshiftbatcher configuration:
```yaml
mask: true
maskSalt: sample-salt
maskConfigDir: "/usr/inventory.yaml"
maskConfigFileName: ""               // default is "database.yaml"
```

### Example config:
Default works like:
```
If,     mask=true            (in redshiftbatcher config)
        maskConfigDir="/usr" (in redshiftbatcher config)
        maskConfigFilePath="" (i.e. not defined)
        topic="datapipe.inventory.customers"
Then,
        configuration file should be present at: /usr/inventory.yaml
```
To not use default file name, specify: `maskConfigFileName`

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

### Mobile Keys
Mobile keys, if specified, the first 4 digits of E164 formatted mobile numbers will be copied into an additional column.
Eg: If mobile_number is `+919812345678`, `+9198` is stored in mobile_number_init5

```yaml
mobile_keys:
    customers:
    - mobile_number
```

### Mapping PII Keys
Mapping PII Keys, if specified adds new columns with the masked values.
Eg: `id` will be as it is and `hashed_id` would be added with masked values.

```yaml
mapping_pii_keys:
    establishments:
    - id
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
