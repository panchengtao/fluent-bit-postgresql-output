# fluent-bit-postgresql-output
fluent bit postgresql output plugin, supportedf by pgx.

# Usage

1. go build -buildmode=c-shared -o out_gstdout.so .
2. ${FlUENTBITPATH}/fluent-bit -e ${PLUGINPATH}/out_gstdout.so -c ${FlUENTBITCONFPATH}/fluent-bit.conf
