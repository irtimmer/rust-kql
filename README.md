# Rust-kql

The rust-kql project is a set of Rust crates for parsing and evaluating [Kusto Query Language (KQL)](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/) queries.
For evaluation, the [DataFusion](https://github.com/apache/arrow-datafusion) engine is used.
Due to differences between the Kusto engine and DataFusion, only a subset of KQL is supported.
Future work may include adding support for DataFusion features that are not present in the original KQL.

A simple command line tool is provided to demonstrate how to use the parser and planner crates.

## kqlparser
The `kqlparser` crate provides a parser for KQL queries. It is based on the `nom` parser library.
See the Status section below for the current state of the parser.
Most simple queries can be parsed, but due to unclearities in the KQL grammar, some queries may not be parsed.

## datafusion-kql
The `datafusion-kql` crate provides a planner to convert parsed KQL queries into DataFusion logical plans.
See the Status section below for the current state of the planner.
Due the differences between Kusto and DataFusion, the development of the planner is going slower than the parser.
Very simple queries can be executed, but uncertain if more complex queries will ever going to work.

## kq
The `kq` crate provides a simple command line tool to show how to use the `kqlparser` and `datafusion-kql` crates.

Example usage of the `kq` command line tool:
```shell
kq -f users.csv 'users | where name == "iwan" and age > 30'
kq -f logins.csv 'logins | summarize count(name) by name'
kq -f users.csv -f logins.csv 'logins | join (users) on name | project name, age, login_time'
```

## Status
✔️ native implemented, like existing DataFusion functions  
✅ (mostly) done  
🚧 partial / in progress  
❌ not started

### Data Types

Type|Parser|Planner|
-|-|-|
bool|✅|✅
datetime|🚧|✅
decimal|🚧[^1]|❌
dynamic|✅|❌
guid|❌|❌
int|✅|✅
long|✅|✅
real|✅|✅
string|✅|✅
timespan|✅|✅

[^1]: Parsed as 64-bit floating number instead of 128-bit

### Operators

Operator|Parser|Planner|
-|-|-|
as|✅|❌
consume|✅|❌
count|✅|✅
datatable|✅|✅
distinct|✅|❌
evaluate|✅|❌
extend|✅|✅
externaldata|✅|❌
facet|✅|❌
find|✅|❌
fork|✅|❌
getschema|✅|✅
join|✅|🚧
lookup|✅|❌
mv-apply|✅|❌
mv-expand|✅|✅
print|✅|✅
project|✅|✅
project-away|✅|✅
project-keep|✅|✅
project-rename|✅|✅
project-reorder|✅|❌
parse|✅|❌
parse-where|✅|❌
parse-kv|✅|❌
partition|✅|❌
range|✅|🚧
reduce|✅|❌
render|✅|❌
sample|✅|❌
sample-distinct|✅|❌
scan|❌|❌
search|❌|❌
serialize|✅|✅
summarize|✅|✅
sort|✅|✅
take|✅|✅
top|✅|✅
top-nested|❌|❌
top-hitters|❌|❌
union|✅|🚧
where|✅|✅

### Statements
Type|Parser|Planner|
-|-|-|
alias|❌|❌
let|✅|❌
pattern|❌|❌
query parameters decleration|❌|❌
restrict|❌|❌
set|❌|❌
tabular expression|✅|🚧

### Scalar Functions

#### Mathematical functions
Function|Implemented
-|-
abs()|✔️
acos()|✔️
asin()|✔️
atan()|✔️
atan2()|✔️
beta_cdf()|❌
beta_inv()|❌
beta_pdf()|❌
cos()|✔️
cot()|✔️
degrees()|✔️
erf()|❌
erfc()|❌
exp()|✔️
exp10()|❌
exp2()|❌
gamma()|❌
isfinite()|❌
isinf()|❌
isnan()|❌
log()|✔️
log10()|✔️
log2()|✔️
loggamma()|❌
not()|❌
pi()|✔️
pow()|✔️
radians()|✔️
rand()|🚧
range()|❌
round()|✔️
sign()|❌
sin()|✔️
sqrt()|✔️
tan()|✔️
welch_test()|❌

#### Conditional functions
Function|Implemented
-|-
case()|❌
coalesce()|✔️
iff()|❌
max_of()|❌
min_of()|❌

#### String functions
Function|Implemented
-|-
base64_encode_tostring()|❌
base64_encode_fromguid()|❌
base64_decode_tostring()|❌
base64_decode_toarray()|❌
base64_decode_toguid()|❌
countof()|❌
extract()|❌
extract_all()|❌
extract_json()|❌
has_any_index()|❌
indexof()|🚧
isempty()|❌
isnotempty()|❌
isnotnull()|❌
isnull()|❌
parse_command_line()|❌
parse_csv()|❌
parse_ipv4()|❌
parse_ipv4_mask()|❌
parse_ipv6()|❌
parse_ipv6_mask()|❌
parse_json()|❌
parse_url()|❌
parse_urlquery()|❌
parse_version()|❌
replace_regex()|✅
replace_string()|✅
replace_strings()|❌
punycode_from_string()|❌
punycode_to_string()|❌
reverse()|✔️
split()|✅
strcat()|✅
strcat_delim()|❌
strcmp()|❌
strlen()|✅
strrep()|🚧
substring()|✅
tohex()|❌
tolower()|✅
toupper()|✅
translate()|❌
trim()|✔️
trim_end()|❌
trim_start()|❌
url_decode()|❌
url_encode()|❌

#### Binary functions
Function|Implemented
-|-
binary_and()|❌
binary_not()|❌
binary_or()|❌
binary_shift_left()|❌
binary_shift_right()|❌
binary_xor()|❌
bitset_count_ones()|❌

#### Conversion functions
Function|Implemented
-|-
tobool()|❌
todatetime()|❌
todecimal()|❌
todouble()|❌
toguid()|❌
toint()|❌
tolong()|❌
tostring()|❌
totimespan()|❌

#### Type functions
Function|Implemented
-|-
gettype()|❌

#### Metadata functions
Function|Implemented
-|-
column_ifexists()|❌
column_cluster_endpoint()|❌
column_database()|❌
current_principal()|❌
current_principal_details()|❌
current_principal_is_member_of()|❌
cursor_after()|❌
estimate_data_size()|❌
extent_id()|❌
extent_tags()|❌
ingestion_time()|❌

#### Window scalar functions
Function|Implemented
-|-
next()|❌
prev()|❌
row_cumsum()|❌
row_number()|✔️
row_rank_dense()|❌
row_rank_min()|❌

#### Rounding functions
Function|Implemented
-|-
bin()|❌
bin_at()|❌
ceiling()|❌

#### Hash functions
Function|Implemented
-|-
hash()|❌
hash_combine()|❌
hash_many()|❌
hash_md5()|❌
hash_sha1()|❌
hash_sha256()|❌
hash_xxhash64()|❌

#### Scalar aggregation functions
Function|Implemented
-|-
dcount_hll()|❌
hll_merge()|❌
percentile_tdigest()|❌
percentile_array_tdigest()|❌
percentrank_tdigest()|❌
rank_tdigest()|❌
merge_tdigest()|❌

#### Flow control functions
Function|Implemented
-|-
toscalar()|❌

#### IP functions
Function|Implemented
-|-
ipv4_compare()|❌
ipv4_is_in_range()|❌
ipv4_is_in_any_range()|❌
ipv4_is_private()|❌
ipv4_netmask_suffix()|❌
ipv4_range_to_cidr_list()|❌
ipv6_compare()|❌
ipv6_is_match()|❌
format_ipv4()|❌
format_ipv4_mask()|❌
ipv6_is_in_range()|❌
ipv6_is_in_any_range()|❌
geo_info_from_ip_address()|❌
has_ipv4()|❌
has_ipv4_prefix()|❌
has_any_ipv4()|❌
has_any_ipv4_prefix()|❌

#### Unit conversion functions
Function|Implemented
-|-
convert_angle()|❌
convert_energy()|❌
convert_force()|❌
convert_length()|❌
convert_mass()|❌
convert_speed()|❌
convert_temperature()|❌
convert_volume()|❌

#### Dynamic/array functions
Function|Implemented
-|-
array_concat()|❌
array_iff()|❌
array_index_of()|❌
array_join()|✔️
array_length()|✔️
array_reverse()|✔️
array_rotate_left()|❌
array_rotate_right()|❌
array_shift_left()|❌
array_shift_right()|❌
array_slice()|✔️
array_sort_asc()|❌
array_sort_desc()|❌
array_split()|❌
array_sum()|❌
bag_has_key()|❌
bag_keys()|❌
bag_merge()|❌
bag_pack()|❌
bag_pack_columns()|❌
bag_remove_keys()|❌
bag_set_key()|❌
jaccard_index()|❌
pack_all()|❌
pack_array()|❌
repeat()|❌
set_difference()|❌
set_has_element()|❌
set_intersect()|❌
set_union()|❌
treepath()|❌
zip()|❌

#### Date and Time functions
Function|Implemented
-|-
ago()|❌
datetime_add()|❌
datetime_diff()|❌
datetime_local_to_utc()|❌
datetime_part()|❌
datetime_utc_to_local()|❌
dayofmonth()|❌
dayofweek()|❌
dayofyear()|❌
endofday()|❌
endofmonth()|❌
endofweek()|❌
endofyear()|❌
format_datetime()|❌
format_timespan()|❌
getyear()|❌
hourofday()|❌
make_datetime()|❌
make_timespan()|❌
monthofyear()|❌
now()|✔️
startofday()|❌ # today
startofmonth()|❌
startofweek()|❌
startofyear()|❌
unixtime_microseconds_todatetime()|❌
unixtime_milliseconds_todatetime()|❌
unixtime_nanoseconds_todatetime()|❌
unixtime_seconds_todatetime()|❌
weekofyear()|❌

#### Series processing functions
Function|Implemented
-|-
series_cosine_similarity()|❌
series_decompose()|❌
series_decompose_anomalies()|❌
series_decompose_forecast()|❌
series_dot_product()|❌
series_fill_backward()|❌
series_fill_constant()|❌
series_fill_forward()|❌
series_fill_linear()|❌
series_fft()|❌
series_fir()|❌
series_fit_2lines()|❌
series_fit_2lines_dynamic()|❌
series_fit_line()|❌
series_fit_line_dynamic()|❌
series_fit_poly()|❌
series_ifft()|❌
series_iir()|❌
series_magnitude()|❌
series_outliers()|❌
series_pearson_correlation()|❌
series_periods_detect()|❌
series_periods_validate()|❌
series_product()|❌
series_seasonal()|❌
series_stats()|❌
series_stats_dynamic()|❌
series_sum()|❌

#### Series element-wise functions
Function|Implemented
-|-
series_abs()|❌
series_acos()|❌
series_add()|❌
series_asin()|❌
series_atan()|❌
series_ceiling()|❌
series_cos()|❌
series_devide()|❌
series_equals()|❌
series_exp()|❌
series_floor()|❌
series_greater()|❌
series_greater_equals()|❌
series_less()|❌
series_less_equals()|❌
series_log()|❌
series_multiply()|❌
series_not_equals()|❌
series_pow()|❌
series_sign()|❌
series_sin()|❌
series_subtract()|❌
series_tan()|❌

#### Geospatial functions
Function|Implemented
-|-
geo_angle()|❌
geo_azimuth()|❌
geo_closest_point_on_line()|❌
geo_closest_point_on_polygon()|❌
geo_distance_2points()|❌
geo_distance_point_to_line()|❌
geo_distance_point_to_polygon()|❌
geo_from_wkt()|❌
geo_intersects_2lines()|❌
geo_intersects_2polygons()|❌
geo_intersects_line_with_polygon()|❌
geo_intersection_2lines()|❌
geo_intersection_2polygons()|❌
geo_intersection_line_with_polygon()|❌
geo_point_buffer()|❌
geo_point_in_circle()|❌
geo_point_in_polygon()|❌
geo_point_to_geohash()|❌
geo_point_to_s2cell()|❌
geo_point_to_h3cell()|❌
geo_line_buffer()|❌
geo_line_centroid()|❌
geo_line_densify()|❌
geo_line_interpolate_point()|❌
geo_line_length()|❌
geo_line_locate_point()|❌
geo_line_simplify()|❌
geo_line_to_s2cells()|❌
geo_polygon_area()|❌
geo_polygon_buffer()|❌
geo_polygon_centroid()|❌
geo_polygon_densify()|❌
geo_polygon_perimeter()|❌
geo_polygon_simplify()|❌
geo_polygon_to_s2cells()|❌
geo_polygon_to_h3cells()|❌
geo_geohash_to_central_point()|❌
geo_geohash_neighbors()|❌
geo_geohash_to_polygon()|❌
geo_s2cell_to_central_point()|❌
geo_s2cell_neighbors()|❌
geo_s2cell_to_polygon()|❌
geo_h3cell_to_central_point()|❌
geo_h3cell_neighbors()|❌
geo_h3cell_to_polygon()|❌
geo_h3cell_parent()|❌
geo_h3cell_children()|❌
geo_h3cell_level()|❌
geo_h3cell_rings()|❌
geo_simplify_polygons_array()|❌
geo_union_lines_array()|❌
geo_union_polygons_array()|❌

### Aggregation functions

#### Statistical functions
Function|Implemented
-|-
avg()|✔️
avgif()|❌
count()|✔️
countif()|❌
count_distinct()|❌
count_distinctif()|❌
dcount()|❌
dcountif()|❌
hll()|❌
hll_if()|❌
hll_merge()|❌
max()|✔️
maxif()|❌
min()|✔️
minif()|❌
percentile()|❌
percentiles()|❌
percentiles_array()|❌
percentilesw()|❌
percentilesw_array()|❌
stdev()|❌
stdevif()|❌
stdevp()|❌
sum()|✔️
sumif()|❌
tdigest()|❌
tdigest_merge()|❌
variance()|❌
varianceif()|❌
variancep()|❌
variancepif()|❌

#### Binary functions
Function|Implemented
-|-
binary_all_and()|❌
binary_all_or()|❌
binary_all_xor()|❌

#### Row selector functions
Function|Implemented
-|-
arg_max()|❌
arg_min()|❌
take_any()|❌
take_anyif()|❌

#### Dynamic functions
Function|Implemented
-|-
buildschema()|❌
make_bag()|❌
make_bag_if()|❌
make_list()|❌
make_list_if()|❌
make_list_with_nulls()|❌
make_set()|❌
make_set_if()|❌