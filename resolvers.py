from analytics.graphql.constants import *
from tools.gql.resolvers import DjangoListResolver
from analytics import models
from django.db.models import Sum, F, Q
from django.db.models.expressions import Window
from django.db.models.functions import RowNumber


class GenericResolver(DjangoListResolver):
    @classmethod
    def preprocess_queryset(
        cls,
        qs,
        source,
        info,
        sr_fields=None,
        pr_fields=None,
        model=None,
        filter_options=None,
        search_options=None,
        order_options=None,
        *args,
        **kwargs,
    ):

        return qs


class AccountWisePodDetailsResolver:
    def __call__(self, source, info, *args, **kwargs):
        try:

            def stringify(data_item):
                # To avoid problems of serialization converting all types to str
                if type(data_item) not in [type("char")]:  # , type(1)]:
                    # To avoid null values to reach FE overriding with 0
                    if data_item is None:
                        data_item = 0
                    return str(data_item)
                else:
                    return data_item

            # function to get current period info
            def get_current_period():
                current_period_info = models.PeriodsFacts.objects.filter(
                    period_tag__iexact="current_period"
                )
                return current_period_info.values()[0]

            account_name_received = kwargs.get(
                "account_name", DEFAULT_ACCOUNT_TO_DISPLAY
            )
            # if NBI is selected get the details of hardcode podnames else the details of specific account name
            if account_name_received == DEFAULT_ACCOUNT_TO_DISPLAY:
                qs = models.MasterPodDetailsFact.objects.filter(pod_name__in=NBI_PODS)
            else:
                qs = models.MasterPodDetailsFact.objects.filter(
                    account_name__iexact=account_name_received
                )
            # taking sample object to group the keys for year wise demarcation
            try:
                sample = qs.values()[0]
            except Exception as e:
                raise Exception("Account Name(s) not found ..!" + str(e))
            # -7 becoz of the mapping difference in periods fact and constants file
            current_period_id = get_current_period()["id"] - 7
            current_period_name = get_current_period()["period_name"].lower()
            # mapping ids that should be +- 6 periods from current period
            considerable_previous = current_period_id - 6
            considerable_future = current_period_id + 6
            # seggregating keys year wise
            current_year_keys = list(
                filter(lambda x: "current_year" in x , sample)
            )
            previous_year_keys = list(
                filter(lambda x: "previous_year" in x , sample)
            )
            next_year_keys = list(filter(lambda x: "next_year" in x , sample))
            remaining_keys = list(
                filter(
                    lambda x: "next_year" not in x 
                    and "previous_year" not in x 
                    and "current_year" not in x ,
                    sample,
                )
            )
            custom_object_list = []
            for row in qs.values():
                _obj = {}
                for k in remaining_keys:
                    _obj[f"{k}"] = stringify(row.get(f"{k}"))
                _prev_obj = {}
                _current_obj = {}
                _next_obj = {}
                for k in previous_year_keys:
                    _prev_obj[f"{k}"] = row.get(f"{k}")
                final_prev = {}
                if considerable_previous < 1:
                    for period in POSSIBLE_PREVIOUS_PERIODS[
                        -(considerable_previous - 1) :
                    ]:
                        final_prev[period] = {
                            k.split("_")[2]: stringify(_prev_obj[k])
                            for k in _prev_obj
                            if period + "_" in k
                        }
                        final_prev[period].update(upcoming=False)
                for k in current_year_keys:
                    _current_obj[f"{k}"] = row.get(f"{k}")
                final_current = {}
                for period in POSSIBLE_CURRENT_PERIODS:
                    final_current[period] = {
                        k.split("_")[2]: stringify(_current_obj[k])
                        for k in _current_obj
                        if period + "_" in k
                    }
                    final_current[period].update(
                        upcoming=(POSSIBLE_CURRENT_PERIODS.index(period)
                        > POSSIBLE_CURRENT_PERIODS.index(current_period_name))
                    )
                    
                for k in next_year_keys:
                    _next_obj[f"{k}"] = row.get(f"{k}")
                final_next = {}
                if considerable_future > 19:
                    for period in POSSIBLE_NEXT_PERIODS[: considerable_future - 19]:
                        final_next[period] = {
                            k.split("_")[2]: stringify(_next_obj[k])
                            for k in _next_obj
                            if period + "_" in k
                        }
                        final_next[period].update(upcoming=True)
                _obj["previousPeriods"] = final_prev
                _obj["currentPeriods"] = final_current
                _obj["nextPeriods"] = final_next
                custom_object_list.append(_obj)
            # grouping by project
            final_result = {}
            for obj in custom_object_list:
                if obj["project_name"] in final_result:
                    final_result[obj["project_name"]].append(obj)
                else:
                    final_result[obj["project_name"]] = [obj]

            return {"ok": True, "error": "", "data": final_result}
        except Exception as e:
            return {"ok": False, "error": f"The error is -> {e} ", "data": []}


class UniqueAccountsResolver:
    def __call__(self, source, info, **kwargs):
        try:
            return {
                "ok": True,
                "error": "",
                "data": [DEFAULT_ACCOUNT_TO_DISPLAY]
                + list(
                    models.MasterPodDetailsFact.objects.order_by()
                    .values_list("account_name", flat=True)
                    .distinct()
                ),
            }
        except Exception as e:
            return {"ok": False, "error": f"The error is -> {e} ", "data": []}


class BenchAgingGrandTotalByCapabilityResolver:
    def __call__(self, source, info, **kwargs):
        # filter by duration
        # group by capability and sum allocation_pending, nominated, bench
        try:
            qs = models.BenchAgingByCapability.objects.filter(duration="ALL_DURATION")

            final_qs = (
                qs.values("capability_name")
                .order_by("capability_name")
                .annotate(
                    no_of_employees=Sum("available_employees")
                    + Sum("nominated_employees")
                    + Sum("allocation_pending_employees"),
                    id=Window(expression=RowNumber(), order_by=[F("capability_name")]),
                )
            )

            return final_qs
        except models.BenchAgingByCapability.DoesNotExist:
            return []


class BenchAgingGrandTotalByBandResolver:
    def __call__(self, source, info, **kwargs):
        # filter by duration
        # group by band and sum allocation_pending, nominated, bench
        try:
            qs = models.BenchAgingByBand.objects.filter(duration="ALL_DURATION")

            final_qs = (
                qs.values("band_name")
                .order_by("band_name")
                .annotate(
                    no_of_employees=Sum("available_employees")
                    + Sum("nominated_employees")
                    + Sum("allocation_pending_employees"),
                    id=Window(expression=RowNumber(), order_by=[F("band_name")]),
                )
            )

            return final_qs
        except models.BenchAgingByBand.DoesNotExist:
            return []


class BaseDataSourceResolverByCapability:
    def __call__(self, source, info, **kwargs):
        # params required
        # 1. capability name (Java,...., Total, Other)
        # 2. start date, end date
        # 3. type of data (Available, Nominated, Allocation Pending, Total bench)
        # 4. band name (Optional)

        capability_name = kwargs.get("capability_name")
        band_name = kwargs.get("band_name")
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        bench_type = kwargs.get(
            "bench_type"
        )  # Allocation Pending, Nominated, Available, Total Bench

        qs_filters = Q(
            updated_end_date__range=(start_date, end_date),
            nomination_status__in=bench_type,
        )

        if None in bench_type:
            qs_filters = qs_filters | Q(nomination_status__isnull=True)

        qs = models.BaseBenchDataSource.objects.filter(qs_filters)

        # validate capability_name
        if str(capability_name).lower() == "total":
            return qs

        if str(capability_name).lower() == "other":
            # TODO:// handle case like "Java,QA"
            qs = qs.filter(capabilities=None)
        else:
            qs = qs.filter(capabilities=capability_name)

        if band_name:
            qs = qs.filter(band=band_name)

        return qs


class TotalBaseDataSourceResolverByCapability:
    def __call__(self, source, info, **kwargs):
        # params required
        # 1. capability name
        # 2. type of data (Available, Nominated, Allocation Pending, Total bench)

        capability_name = kwargs.get("capability_name")
        bench_type = kwargs.get("bench_type")

        qs_filters = Q(nomination_status__in=bench_type, updated_end_date__isnull=False)

        if None in bench_type:
            qs_filters = qs_filters | Q(nomination_status__isnull=True)

        qs = models.BaseBenchDataSource.objects.filter(qs_filters)

        if str(capability_name).lower() == "other":
            # TODO:// handle case like "Java,QA"

            return qs.filter(capabilities=None)
        elif str(capability_name).lower() == "total":
            return qs

        return qs.filter(capabilities=capability_name)


class BaseDataSourceResolverByBand:
    def __call__(self, source, info, **kwargs):
        # params required
        # 1. band name
        # 2. start date, end date
        # 3. type of data (Available, Nominated, Allocation Pending, Total bench)

        band_name = kwargs.get("band_name")
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        bench_type = kwargs.get(
            "bench_type"
        )  # Allocation Pending, Nominated, Available, Total Bench

        qs_filters = Q(
            updated_end_date__range=(start_date, end_date),
            nomination_status__in=bench_type,
        )

        if None in bench_type:
            qs_filters = qs_filters | Q(nomination_status__isnull=True)

        qs = models.BaseBenchDataSource.objects.filter(qs_filters)

        # validate capability_name
        if str(band_name).lower() == "total":
            return qs
        else:
            qs = qs.filter(band=band_name)

        return qs


class TotalBaseDataSourceResolverByBand:
    def __call__(self, source, info, **kwargs):
        # params required
        # 1. band name
        # 2. type of data (Available, Nominated, Allocation Pending, Total bench)

        band_name = kwargs.get("band_name")
        bench_type = kwargs.get("bench_type")

        qs_filters = Q(nomination_status__in=bench_type, updated_end_date__isnull=False)

        if None in bench_type:
            qs_filters = qs_filters | Q(nomination_status__isnull=True)

        qs = models.BaseBenchDataSource.objects.filter(qs_filters)

        if str(band_name).lower() == "total":
            return qs
        else:
            qs = qs.filter(band=band_name)

        return qs
