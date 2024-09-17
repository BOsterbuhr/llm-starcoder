import os
import shutil

import pachyderm_sdk
from pachyderm_sdk.api.pfs import File, FileType
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from requests.exceptions import HTTPError

# ======================================================================================================================



def safe_open_wb(path):
    ''' Open "path" for writing, creating any parent directories as needed.
    '''
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return open(path, 'wb')


# Adding retry logic to handle 503 errors
@retry(
    retry=retry_if_exception_type(HTTPError),
    stop=stop_after_attempt(5),  # Retry up to 5 times
    wait=wait_exponential(multiplier=1, min=2, max=60),  # Exponential backoff
)


def download_pach_repo(
    pachyderm_host,
    pachyderm_port,
    repo,
    branch,
    root,
    token,
    fileset_id,
    datum_id,
    cache_location
):
    print(f"Starting to download dataset: {repo}@{branch} --> {root}")
    datum_path = f"/pfs/{datum_id}"
    print(f"Downloading {datum_path} to {root}")
    if not os.path.exists(root):
        os.makedirs(root)

    if not os.path.exists(f"{root}/pfs/{datum_id}"):
        os.makedirs(f"{root}/pfs/{datum_id}")

    cert = b"""-----BEGIN CERTIFICATE-----
MIIFCTCCA/GgAwIBAgISBF9N5RfkbTMW/oW8belXLilNMA0GCSqGSIb3DQEBCwUA
MDMxCzAJBgNVBAYTAlVTMRYwFAYDVQQKEw1MZXQncyBFbmNyeXB0MQwwCgYDVQQD
EwNSMTAwHhcNMjQwODI1MDExMzQ0WhcNMjQxMTIzMDExMzQzWjAVMRMwEQYDVQQD
EwpwYWNocGUuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtgnI
ehlw8SJVUpsKqYtylo7GYuprGLG9F6Q339dgBE9p2dB4QDNYF1IxMafbfZA6Q2Nc
mDhJ4Hq2FgSIgPrTd/RxMCdrUhPYedirjYt6j8mYdT35MGUEHn90VBWHWVvdwcI7
0gsnHCK2Hllz2Zxx+jAtJPFBPl3lN0MwzEsSsExBrbWIFcQaYN+4P41DAquyccVD
zYJKVgjmdbygfQ7sOqeLhp8OJlVG459w3Hr9N5Ez2UjNfwulXyDe9LGp+ABZsMFo
GZYbWVDkZ/UPnIO/uHTH3RR8EUFDBerZwfjyRfWVHyfKUhkq82NtNersmod8B9xr
X14qU2ePceMp02f0FQIDAQABo4ICMzCCAi8wDgYDVR0PAQH/BAQDAgWgMB0GA1Ud
JQQWMBQGCCsGAQUFBwMBBggrBgEFBQcDAjAMBgNVHRMBAf8EAjAAMB0GA1UdDgQW
BBRhh3vTP3emnvtBLXkWCY0rvV5XTjAfBgNVHSMEGDAWgBS7vMNHpeS8qcbDpHIM
EI2iNeHI6DBXBggrBgEFBQcBAQRLMEkwIgYIKwYBBQUHMAGGFmh0dHA6Ly9yMTAu
by5sZW5jci5vcmcwIwYIKwYBBQUHMAKGF2h0dHA6Ly9yMTAuaS5sZW5jci5vcmcv
MDkGA1UdEQQyMDCCEmNvbnNvbGUucGFjaHBlLmNvbYIOZGV0LnBhY2hwZS5jb22C
CnBhY2hwZS5jb20wEwYDVR0gBAwwCjAIBgZngQwBAgEwggEFBgorBgEEAdZ5AgQC
BIH2BIHzAPEAdgBIsONr2qZHNA/lagL6nTDrHFIBy1bdLIHZu7+rOdiEcwAAAZGH
TjHxAAAEAwBHMEUCIQDMDXfjgBazqT1k6c+e8GsP4OEUw9XirA4BNjTEkRwdgQIg
HDEcFbfQ8My9bhx7w5VGHkacKvupwsMNlztp7gdZU8sAdwA/F0tP1yJHWJQdZRyE
vg0S7ZA3fx+FauvBvyiF7PhkbgAAAZGHTjH3AAAEAwBIMEYCIQChIHM0KGwBu/pw
eJyjYxE3tyeVcGDbnR3TBHivtzmFIgIhAPUIenWZboKJOsqNvq9N5RJ9MDXFFLTV
9uTXt9qb9X0fMA0GCSqGSIb3DQEBCwUAA4IBAQCIl4KiXOys5+57RJQg7QGBx454
RUT5WRXHOvVgKnxI0tUpXJ2A7AN30uEqN7pr6EYp6gtRUEeZ3oig7tHbGlmpsfgG
ONPXKxXZJkYuuomK/zXberEjNPltVHgbiuex6dxcNbOwAzDErq4daOzpxVN8Y6OE
hKtWM4TgKU45Sa7dZjT+VdSt38ItgTF6FQ0E5EssHoOSiJUwAeJU8PRenmeeW/PJ
Bvm5+c4CBvSV1Tbf3hkRzT5qNw0qXcQjR5DCIai3sBQCUrd3oa8OcjKag9pieN5Y
g78SUt6dLz2NsH6s1rAzgId8gyWOSF8BxgoSyAplbW9nrA5JBsdy+dKLeMh3
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIFBTCCAu2gAwIBAgIQS6hSk/eaL6JzBkuoBI110DANBgkqhkiG9w0BAQsFADBP
MQswCQYDVQQGEwJVUzEpMCcGA1UEChMgSW50ZXJuZXQgU2VjdXJpdHkgUmVzZWFy
Y2ggR3JvdXAxFTATBgNVBAMTDElTUkcgUm9vdCBYMTAeFw0yNDAzMTMwMDAwMDBa
Fw0yNzAzMTIyMzU5NTlaMDMxCzAJBgNVBAYTAlVTMRYwFAYDVQQKEw1MZXQncyBF
bmNyeXB0MQwwCgYDVQQDEwNSMTAwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK
AoIBAQDPV+XmxFQS7bRH/sknWHZGUCiMHT6I3wWd1bUYKb3dtVq/+vbOo76vACFL
YlpaPAEvxVgD9on/jhFD68G14BQHlo9vH9fnuoE5CXVlt8KvGFs3Jijno/QHK20a
/6tYvJWuQP/py1fEtVt/eA0YYbwX51TGu0mRzW4Y0YCF7qZlNrx06rxQTOr8IfM4
FpOUurDTazgGzRYSespSdcitdrLCnF2YRVxvYXvGLe48E1KGAdlX5jgc3421H5KR
mudKHMxFqHJV8LDmowfs/acbZp4/SItxhHFYyTr6717yW0QrPHTnj7JHwQdqzZq3
DZb3EoEmUVQK7GH29/Xi8orIlQ2NAgMBAAGjgfgwgfUwDgYDVR0PAQH/BAQDAgGG
MB0GA1UdJQQWMBQGCCsGAQUFBwMCBggrBgEFBQcDATASBgNVHRMBAf8ECDAGAQH/
AgEAMB0GA1UdDgQWBBS7vMNHpeS8qcbDpHIMEI2iNeHI6DAfBgNVHSMEGDAWgBR5
tFnme7bl5AFzgAiIyBpY9umbbjAyBggrBgEFBQcBAQQmMCQwIgYIKwYBBQUHMAKG
Fmh0dHA6Ly94MS5pLmxlbmNyLm9yZy8wEwYDVR0gBAwwCjAIBgZngQwBAgEwJwYD
VR0fBCAwHjAcoBqgGIYWaHR0cDovL3gxLmMubGVuY3Iub3JnLzANBgkqhkiG9w0B
AQsFAAOCAgEAkrHnQTfreZ2B5s3iJeE6IOmQRJWjgVzPw139vaBw1bGWKCIL0vIo
zwzn1OZDjCQiHcFCktEJr59L9MhwTyAWsVrdAfYf+B9haxQnsHKNY67u4s5Lzzfd
u6PUzeetUK29v+PsPmI2cJkxp+iN3epi4hKu9ZzUPSwMqtCceb7qPVxEbpYxY1p9
1n5PJKBLBX9eb9LU6l8zSxPWV7bK3lG4XaMJgnT9x3ies7msFtpKK5bDtotij/l0
GaKeA97pb5uwD9KgWvaFXMIEt8jVTjLEvwRdvCn294GPDF08U8lAkIv7tghluaQh
1QnlE4SEN4LOECj8dsIGJXpGUk3aU3KkJz9icKy+aUgA+2cP21uh6NcDIS3XyfaZ
QjmDQ993ChII8SXWupQZVBiIpcWO4RqZk3lr7Bz5MUCwzDIA359e57SSq5CCkY0N
4B6Vulk7LktfwrdGNVI5BsC9qqxSwSKgRJeZ9wygIaehbHFHFhcBaMDKpiZlBHyz
rsnnlFXCb5s8HKn5LsUgGvB24L7sGNZP2CX7dhHov+YhD+jozLW2p9W4959Bz2Ei
RmqDtmiXLnzqTpXbI+suyCsohKRg6Un0RC47+cpiVwHiXZAW+cn8eiNIjqbVgXLx
KPpdzvvtTnOPlC7SQZSYmdunr3Bf9b77AiC/ZidstK36dRILKz7OA54=
-----END CERTIFICATE-----
"""
    client = pachyderm_sdk.Client(
        host=pachyderm_host, port=pachyderm_port, auth_token=token, tls=True, root_certs=cert
    )
    
    try:
        client.storage.assemble_fileset(
            fileset_id,
            path=datum_path,
            destination=root,
            cache_location=cache_location,
        )
    except HTTPError as e:
        if e.response.status_code == 503:
            print("503 error encountered during download. Retrying...")
            raise  # Trigger retry for 503 errors
        else:
            raise  # Raise any other errors

    # Move the files to the root directory and remove the pfs/datum_id directory
    for file in os.listdir(f"{root}/pfs/{datum_id}"):
        print(f"Moving {file} from {root}/pfs/{datum_id} to {root}")
        shutil.move(f"{root}/pfs/{datum_id}/{file}", f"{root}/{file}")

    return [(os.path.join(root, file), file) for file in os.listdir(root)]



# ========================================================================================================
