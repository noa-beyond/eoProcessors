cl=$(< /run/secrets/COPERNICUS_LOGIN)
cp=$(< /run/secrets/COPERNICUS_PASSWORD)
el=$(< /run/secrets/EARTHDATA_LOGIN)
ep=$(< /run/secrets/EARTHDATA_PASSWORD)

credentials_copernicus="machine https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token login $cl password $cp"
credentials_earthdata="machine urs.earthdata.nasa.gov login $el password $ep"
file="${HOME}/.netrc"

echo "$credentials_copernicus" >> "$file"
echo "$credentials_earthdata" >> "$file"
chmod 600 "$file"