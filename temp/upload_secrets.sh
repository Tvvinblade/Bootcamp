#!/bin/bash

# путь к файлу .env
ENV_FILE=".env"

# имя репозитория в формате owner/repo
REPO="your-org/your-repo"

while IFS='=' read -r key value || [ -n "$key" ]; do
  # пропускаем пустые строки и комментарии
  [[ "$key" =~ ^#.*$ ]] && continue
  [[ -z "$key" ]] && continue

  # убираем возможные кавычки вокруг значения
  value=$(echo "$value" | sed -e 's/^"//' -e 's/"$//')

  echo "Setting secret $key"

  # загружаем в GitHub Secret
  gh secret set "$key" --repo "$REPO" --body "$value"
done < "$ENV_FILE"