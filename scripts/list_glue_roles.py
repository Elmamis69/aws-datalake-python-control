import boto3

# Este script lista los roles de IAM que contienen 'glue' en el nombre y muestra su ARN.

iam = boto3.client('iam')
response = iam.list_roles()

found = False
for role in response['Roles']:
    if 'glue' in role['RoleName'].lower():
        print(f"Role: {role['RoleName']}")
        print(f"ARN:  {role['Arn']}\n")
        found = True

if not found:
    print("No se encontraron roles con 'glue' en el nombre. Crea uno en la consola de AWS si es necesario.")
