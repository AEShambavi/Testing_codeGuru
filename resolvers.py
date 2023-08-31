from analytics.graphql.constants import POSSIBLE_PREVIOUS_PERIODS, POSSIBLE_CURRENT_PERIODS, POSSIBLE_NEXT_PERIODS, DEFAULT_ACCOUNT_TO_DISPLAY, ON_BENCH, BENCH_REPORTS_BAND
from analytics.graphql.exceptions import AccountNameNotFound, PodIdNotFound
from tools.gql.resolvers import DjangoListResolver
from analytics import models
from django.db.models import Sum, F, Q, Count
from django.db.models.expressions import Window
from django.db.models.functions import RowNumber
from datetime import date, timedelta
from itertools import chain
from analytics.graphql.utils import custom_resolver_check_perm, check_permissions, b2b_get_pod_details_service, error_helper
from .constants import BaseCapabilities
import json
from graphql import GraphQLError
from django.core.serializers.json import DjangoJSONEncoder
from django.core.exceptions import FieldError
from core import models as core_models


class GenericResolver(DjangoListResolver):
    @classmethod
    @check_permissions
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


class AccountWiseHelper:
    def get_object_name(content_type, file_extension=None, type=None, file_name=None):
     # To avoid problems of serialization converting all types to str
        if type(data_item) not in [type("char")]:  # , type(1)]:
            # To avoid null values to reach FE overriding with 0
            if data_item is None:
                data_item = 0
            return str(data_item)
        else:
            return data_item
    
    def stringify(self, data_item):
        get_object_name('a', None, None, None, 'abcde')
        # To avoid problems of serialization converting all types to str
        if type(data_item) not in [type("char")]:  # , type(1)]:
            # To avoid null values to reach FE overriding with 0
            if data_item is None:
                data_item = 0
            return str(data_item)
        else:
            return data_item

    def cal_final_result(self, custom_object_list):
        final_result = {}
        for obj in custom_object_list:
            if obj["project_name"] in final_result:
                final_result[obj["project_name"]].append(obj)
            else:
                final_result[obj["project_name"]] = [obj]
        return final_result

    def cal_obj(self, row, remaining_keys):
        _obj = {}
        for k in remaining_keys:
            _obj[f"{k}"] = self.stringify(row.get(f"{k}"))
        return _obj

    def cal_prev_obj(self, row, previous_year_keys):
        _prev_obj = {}
        for k in previous_year_keys:
            _prev_obj[f"{k}"] = row.get(f"{k}")
        return _prev_obj

    def cal_current_obj(self, row, current_year_keys):
        _current_obj = {}
        for k in current_year_keys:
            _current_obj[f"{k}"] = row.get(f"{k}")
        return _current_obj

    def cal_next_obj(self, row, next_year_keys):
        _next_obj = {}
        for k in next_year_keys:
            _next_obj[f"{k}"] = row.get(f"{k}")
        return _next_obj

    def calc_final_prev(self, considerable_previous, _prev_obj):
        final_prev = {}
        if considerable_previous < 1:
            for period in POSSIBLE_PREVIOUS_PERIODS[-(considerable_previous - 1):]:
                final_prev[period] = {
                    k.split("_")[2]: self.stringify(_prev_obj[k])
                    for k in _prev_obj
                    if period + "_" in k
                }
                final_prev[period].update(upcoming=False)
        return final_prev

    def calc_final_current(self, _current_obj, current_period_name):
        final_current = {}
        for period in POSSIBLE_CURRENT_PERIODS:
            final_current[period] = {
                k.split("_")[2]: self.stringify(_current_obj[k])
                for k in _current_obj
                if period + "_" in k
            }
            final_current[period].update(
                upcoming=(
                    POSSIBLE_CURRENT_PERIODS.index(period)
                    > POSSIBLE_CURRENT_PERIODS.index(current_period_name)
                )
            )
        return final_current

    def calc_final_next(self, considerable_future, _next_obj):
        final_next = {}
        if considerable_future > 19:
            for period in POSSIBLE_NEXT_PERIODS[: considerable_future - 19]:
                final_next[period] = {
                    k.split("_")[2]: self.stringify(_next_obj[k])
                    for k in _next_obj
                    if period + "_" in k
                }
                final_next[period].update(upcoming=True)
        return final_next

    def cal_custom_obj_list(
        self,
        qs,
        current_period_name,
        considerable_previous,
        considerable_future,
        current_year_keys,
        previous_year_keys,
        next_year_keys,
        remaining_keys,
    ):
        custom_object_list = []
        for row in qs.values():
            _obj = self.cal_obj(row, remaining_keys)

            _prev_obj = self.cal_prev_obj(row, previous_year_keys)
            final_prev = self.calc_final_prev(considerable_previous, _prev_obj)

            _current_obj = self.cal_current_obj(row, current_year_keys)
            final_current = self.calc_final_current(
                _current_obj, current_period_name)

            _next_obj = self.cal_next_obj(row, next_year_keys)
            final_next = self.calc_final_next(considerable_future, _next_obj)

            _obj["previousPeriods"] = final_prev
            _obj["currentPeriods"] = final_current
            _obj["nextPeriods"] = final_next

            custom_object_list.append(_obj)

        return custom_object_list


class AccountWisePodDetailsResolver(AccountWiseHelper):
    @custom_resolver_check_perm
    def __call__(self, source, info, *args, **kwargs):
        try:
            # function to get current period info
            def get_current_period():
                current_period_info = models.Periods.objects.filter(
                    period_tag__iexact="current_period"
                )
                return current_period_info.values()[0]

            account_name_received = kwargs.get(
                "account_name", DEFAULT_ACCOUNT_TO_DISPLAY
            )
            # if NBI is selected get the details of hardcode pod names else the details of specific account name
            if account_name_received == DEFAULT_ACCOUNT_TO_DISPLAY:
                qs = models.MasterPodDetailsFact.objects.filter(
                    pod_type__iexact=DEFAULT_ACCOUNT_TO_DISPLAY
                )
            else:
                qs = models.MasterPodDetailsFact.objects.filter(
                    account_name__iexact=account_name_received
                )
            # taking sample object to group the keys for year wise demarcation
            try:
                sample = qs.values()[0]
            except Exception as e:
                raise AccountNameNotFound(
                    "Account Name(s) not found ..!" + str(e))
            # -7 because of the mapping difference in periods fact and constants file
            current_period_id = get_current_period()["id"] - 7
            current_period_name = get_current_period()["period_name"].lower()
            # mapping ids that should be +- 6 periods from current period
            considerable_previous = current_period_id - 6
            considerable_future = current_period_id + 6
            # Segregating keys year wise
            current_year_keys = list(
                filter(lambda x: "current_year" in x, sample))
            previous_year_keys = list(
                filter(lambda x: "previous_year" in x, sample))
            next_year_keys = list(filter(lambda x: "next_year" in x, sample))
            remaining_keys = list(
                filter(
                    lambda x: "next_year" not in x
                    and "previous_year" not in x
                    and "current_year" not in x,
                    sample,
                )
            )
            custom_object_list = self.cal_custom_obj_list(
                qs,
                current_period_name,
                considerable_previous,
                considerable_future,
                current_year_keys,
                previous_year_keys,
                next_year_keys,
                remaining_keys,
            )
            final_result = self.cal_final_result(custom_object_list)
            return {"ok": True, "error": "", "data": final_result}
        except Exception as e:
            return {"ok": False, "error": f"The error is -> {e} ", "data": []}


class UniqueAccountsResolver:
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        return {"account_name": [DEFAULT_ACCOUNT_TO_DISPLAY] + list(
            models.MasterPodDetailsFact.objects.order_by()
            .values_list("account_name", flat=True)
            .distinct())
        }


class BenchAgingGrandTotalByCapabilityResolver:
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        # filter by duration
        # group by capability and sum allocation_pending, nominated, bench
        try:
            qs = models.BenchAgingByCapability.objects.filter(
                duration="ALL_DURATION")

            final_qs = (
                qs.values("capability_name")
                .order_by("capability_name")
                .annotate(
                    no_of_employees=Sum("available_employees")
                    + Sum("nominated_employees")
                    + Sum("allocation_pending_employees"),
                    id=Window(expression=RowNumber(),
                              order_by=[F("capability_name")]),
                )
            )

            return final_qs
        except models.BenchAgingByCapability.DoesNotExist:
            return []


class BenchAgingGrandTotalByBandResolver:
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        # filter by duration
        # group by band and sum allocation_pending, nominated, bench
        try:
            qs = models.BenchAgingByBand.objects.filter(
                duration="ALL_DURATION")

            final_qs = (
                qs.values("band_name")
                .order_by("band_name")
                .annotate(
                    no_of_employees=Sum("available_employees")
                    + Sum("nominated_employees")
                    + Sum("allocation_pending_employees"),
                    id=Window(expression=RowNumber(),
                              order_by=[F("band_name")]),
                )
            )

            return final_qs
        except models.BenchAgingByBand.DoesNotExist:
            return []


class BaseDataSourceResolverByCapability:
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        # params required
        # 1. capability name (Java,...., Total, Other)
        # 2. start date, end date
        # 3. type of data (Available, Nominated, Allocation Pending, Total bench)
        # 4. band name (Optional)updated_end_date__range=(start_date, end_date),

        capability_name = kwargs.get("capability_name")
        band_name = kwargs.get("band_name")
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        bench_type = kwargs.get(
            "bench_type"
        )  # Allocation Pending, Nominated, Available, Total Bench

        qs_filters = Q(nomination_status__in=bench_type.value)

        if None in bench_type.value:
            qs_filters = qs_filters | Q(nomination_status__isnull=True)

        qs = models.BaseBenchDataSource.objects.filter(qs_filters)
        qs = qs.filter(updated_end_date__range=(start_date, end_date))
        qs = qs.filter(band__in=BENCH_REPORTS_BAND,
                       on_bench__in=ON_BENCH, allocation_end_dates__isnull=True)
        qs = qs.filter(department='Delivery')

        # validate capability_name
        if str(capability_name).lower() == "total":
            return qs

        if str(capability_name).lower() == "other":
            # handle case like "Java,QA"
            qs = qs.filter(capabilities=None)
        else:
            qs = qs.filter(capabilities=capability_name)

        if band_name:
            qs = qs.filter(band=band_name)

        return qs


class TotalBaseDataSourceResolverByCapability:
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        # params required
        # 1. capability name
        # 2. type of data (Available, Nominated, Allocation Pending, Total bench)

        capability_name = kwargs.get("capability_name")
        bench_type = kwargs.get("bench_type")

        qs_filters = Q(nomination_status__in=bench_type.value)

        if None in bench_type.value:
            qs_filters = qs_filters | Q(nomination_status__isnull=True)

        qs = models.BaseBenchDataSource.objects.filter(qs_filters)
        qs = qs.filter(updated_end_date__isnull=False)
        qs = qs.filter(band__in=BENCH_REPORTS_BAND,
                       on_bench__in=ON_BENCH, allocation_end_dates__isnull=True)
        qs = qs.filter(department='Delivery')

        if str(capability_name).lower() == "other":
            # handle case like "Java,QA"
            return qs.filter(capabilities=None)
        elif str(capability_name).lower() == "total":
            return qs

        return qs.filter(capabilities=capability_name)


class BaseDataSourceResolverByBand:
    @custom_resolver_check_perm
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

        qs_filters = Q(nomination_status__in=bench_type.value)

        if None in bench_type.value:
            qs_filters = qs_filters | Q(nomination_status__isnull=True)

        qs = models.BaseBenchDataSource.objects.filter(qs_filters)
        qs = qs.filter(updated_end_date__range=(start_date, end_date))
        qs = qs.filter(band__in=BENCH_REPORTS_BAND,
                       on_bench__in=ON_BENCH, allocation_end_dates__isnull=True)
        qs = qs.filter(department='Delivery')

        # validate capability_name
        if str(band_name).lower() == "total":
            return qs
        else:
            qs = qs.filter(band=band_name)

        return qs


class TotalBaseDataSourceResolverByBand:
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        # params required
        # 1. band name
        # 2. type of data (Available, Nominated, Allocation Pending, Total bench)

        band_name = kwargs.get("band_name")
        bench_type = kwargs.get("bench_type")

        qs_filters = Q(nomination_status__in=bench_type.value)

        if None in bench_type.value:
            qs_filters = qs_filters | Q(nomination_status__isnull=True)

        qs = models.BaseBenchDataSource.objects.filter(qs_filters)
        qs = qs.filter(updated_end_date__isnull=False)
        qs = qs.filter(band__in=BENCH_REPORTS_BAND,
                       on_bench__in=ON_BENCH, allocation_end_dates__isnull=True)
        qs = qs.filter(department='Delivery')

        if str(band_name).lower() == "total":
            return qs
        else:
            qs = qs.filter(band=band_name)

        return qs


# Cell Details Resolver for column names "Allocation Pending", "Nominated", "Bench", "Deployable Total (Grand Total)"
# for Overall Bench View By Band

class BaseDataSourceResolverForOverallBenchByBandDeployable:
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        # params required
        # 1. band name
        # 2. column name (Allocation_Pending, Nominated, Bench, Deployable_Total)

        band_name = kwargs.get("band_name")
        column_name = kwargs.get("column_name")

        qs_filters = Q(nomination_status__in=column_name.value)

        if None in column_name.value:
            qs_filters = qs_filters | Q(nomination_status__isnull=True)

        qs = models.BaseBenchDataSource.objects.filter(qs_filters)
        qs = qs.filter(updated_end_date__isnull=False)
        qs = qs.filter(band__in=BENCH_REPORTS_BAND,
                       on_bench__in=ON_BENCH, allocation_end_dates__isnull=True)
        qs = qs.filter(department='Delivery')

        if str(band_name).lower() == 'total':
            return qs
        else:
            qs = qs.filter(band=band_name)

        return qs


# Cell Details Resolver for column names "Resigned", "LOP", "Maternity", "Non Deployable Total (Grand Total)"
# for Overall Bench View By Band

class BaseDataSourceResolverForOverallBenchByBandNonDeployable:
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        # params required
        # 1. band name
        # 2. column (resigned, loss of pay, maternity, non_deployable_total)

        band_name = kwargs.get("band_name")
        column_name = kwargs.get("column_name")
        today = date.today()
        cw_start = today - timedelta(days=today.weekday() + 2)
        cw_end = cw_start + timedelta(days=6)
        qs_filters_resigned = (Q(resigned_on__lte=today, exit_date__gt=today)) | (
            Q(resigned_on__lte=today, exit_date__isnull=True)
        )
        qs_filters_lop = Q(
            leave_type="loss of pay", leave_end_date__gte=cw_start, leave_start_date__lte=cw_end
        )
        qs_filters_maternity = Q(
            leave_type="maternity",
            leave_end_date__gte=cw_start,
            leave_start_date__lte=cw_end,
        )

        if column_name.value == "RESIGNED":
            qs_filters = qs_filters_resigned

        if column_name.value == "LOP":
            qs_filters = qs_filters_lop

        if column_name.value == "MATERNITY":
            qs_filters = qs_filters_maternity

        if column_name.value == "NON_DEPLOYABLE_TOTAL":
            qs_filters = qs_filters_resigned | qs_filters_lop | qs_filters_maternity

        qs = models.BaseBenchDataSource.objects.filter(qs_filters)
        qs = qs.filter(band__in=BENCH_REPORTS_BAND,
                       on_bench__in=ON_BENCH, allocation_end_dates__isnull=True)
        qs = qs.filter(department='Delivery')

        if str(band_name).lower() == 'total':
            return qs

        else:
            qs = qs.filter(band=band_name)

        return qs


# Cell Details Resolver for column names "Allocation Pending", "Nominated", "Bench", "Deployable Total (Grand Total)"
# for Overall Bench View By Capability

class BaseDataSourceResolverForOverallBenchByCapabilityDeployable:
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        # params required
        # 1. capability name
        # 2. column (Allocation Pending, Nominated, Bench, Deployable_Total)

        capability_name = kwargs.get("capability_name")
        column_name = kwargs.get("column_name")

        qs_filters = Q(nomination_status__in=column_name.value)

        if None in column_name.value:
            qs_filters = qs_filters | Q(nomination_status__isnull=True)

        qs = models.BaseBenchDataSource.objects.filter(qs_filters)
        qs = qs.filter(updated_end_date__isnull=False)
        qs = qs.filter(band__in=BENCH_REPORTS_BAND,
                       on_bench__in=ON_BENCH, allocation_end_dates__isnull=True)
        qs = qs.filter(department='Delivery')

        if str(capability_name).lower() == 'other':
            # handle case like "Java,QA"

            return qs.filter(capabilities=None)
        elif str(capability_name).lower() == "total":
            return qs

        return qs.filter(capabilities=capability_name)


# Cell Details Resolver for column names "Resigned", "LOP", "Maternity", "Non Deployable Total (Grand Total)"
# for Overall Bench View By Capability

class BaseDataSourceResolverForOverallBenchByCapabilityNonDeployable:
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        # params required
        # 1. capability name
        # 2. column (Resigned, loss of pay, maternity)

        capability_name = kwargs.get("capability_name")
        column_name = kwargs.get("column_name")
        today = date.today()
        cw_start = today - timedelta(days=today.weekday() + 2)
        cw_end = cw_start + timedelta(days=6)
        qs_filters_resigned = (Q(resigned_on__lte=today, exit_date__gt=today)) | (
            Q(resigned_on__lte=today, exit_date__isnull=True)
        )
        qs_filters_lop = Q(
            leave_type="loss of pay", leave_end_date__gte=cw_start, leave_start_date__lte=cw_end
        )
        qs_filters_maternity = Q(
            leave_type="maternity",
            leave_end_date__gte=cw_start,
            leave_start_date__lte=cw_end,
        )

        if column_name.value == "RESIGNED":
            qs_filters = qs_filters_resigned

        if column_name.value == "LOP":
            qs_filters = qs_filters_lop

        if column_name.value == "MATERNITY":
            qs_filters = qs_filters_maternity

        if column_name.value == "NON_DEPLOYABLE_TOTAL":
            qs_filters = qs_filters_resigned | qs_filters_lop | qs_filters_maternity

        qs = models.BaseBenchDataSource.objects.filter(qs_filters)
        qs = qs.filter(band__in=BENCH_REPORTS_BAND,
                       on_bench__in=ON_BENCH, allocation_end_dates__isnull=True)
        qs = qs.filter(department='Delivery')

        if str(capability_name).lower() == 'other':
            # handle case like "Java,QA"

            return qs.filter(capabilities=None)
        elif str(capability_name).lower() == "total":
            return qs

        return qs.filter(capabilities=capability_name)


class InflowOutflowHelper:
    def __init__(self):
        self.department_name = ["Delivery"]
        self.excluded_band = ["B2", "B3H", "B3L", "B4H", "B4L"]
        self.on_bench_inclusion = ["true", "N/A"]

    def get_qs_outflow_allocation_facts(self, start_date, end_date):
        qs = models.AllocationsFacts.objects.filter(Q(start_date__range=(start_date, end_date)) & Q(department__in=self.department_name) & ~Q(band__in=self.excluded_band)).values('name', 'email', 'band', 'designation', 'department', 'parent_capabilities',
                                                                                                                                                                                   'end_date', 'start_date', 'empid', 'pod_id', 'project', 'account').annotate(capabilities=F('parent_capabilities'), allocation_start_dates=F('start_date'), allocation_end_dates=F('end_date'), employee_id=F('empid'))
        return qs

    def get_qs_inflow_allocation_facts(self, start_date, end_date):
        qs = models.AllocationsFacts.objects.filter(Q(end_date__range=(start_date, end_date)) & Q(department__in=self.department_name) & ~Q(band__in=self.excluded_band)).values('name', 'email', 'band', 'designation', 'department', 'parent_capabilities',
                                                                                                                                                                                 'end_date', 'start_date', 'empid', 'pod_id', 'project', 'account').annotate(capabilities=F('parent_capabilities'), allocation_start_dates=F('start_date'), allocation_end_dates=F('end_date'), employee_id=F('empid'))
        return qs

    def get_qs_training_facts(self, start_date, end_date):
        qs = models.TrainingFacts.objects.filter(training_end_date__range=(start_date, end_date)).distinct(
            'email').values('email', 'training_name', 'training_start_date', 'training_end_date')
        return qs

    def get_qs_profile_facts(self, start_date, end_date):
        qs = models.ProfilesFacts.objects.filter(Q(exit_date__range=(start_date, end_date)) & Q(department__in=self.department_name) & ~Q(band__in=self.excluded_band)).distinct('email').values('full_name', 'email', 'band', 'designation', 'department', 'capabilities',
                                                                                                                                                                                                 'resignation_date', 'exit_date', 'date_of_joining', 'allocation_end_date', 'allocation_start_date').annotate(name=F('full_name'), resigned_on=F('resignation_date'), allocation_start_dates=F('allocation_start_date'), allocation_end_dates=F('allocation_end_date'))
        return qs

    def get_qs_base_bench_facts(self, start_date):
        qs = models.BaseBenchDataSource.objects.filter(Q(updated_end_date__lte=start_date) & Q(department__in=self.department_name) & ~Q(band__in=self.excluded_band) & Q(on_bench__in=self.on_bench_inclusion) & Q(allocation_end_dates__isnull=True)).distinct('email').values(
            'employee_id', 'full_name', 'capabilities', 'email', 'band', 'designation', 'exit_date', 'date_of_joining', 'updated_end_date', 'on_bench', 'resigned_on', 'pod_ids', 'current_projects', 'client_name').annotate(name=F('full_name'), pod_id=F('pod_ids'), project=F('current_projects'), account=F('client_name'))
        return qs


class CapabilityFilter:
    def get_qs_with_capability_filter(
        self, qs, capability_name
    ):
        if str(capability_name).lower() == "other":
            # handle case like "Java,QA"
            return qs.filter(Q(capabilities=None))

        elif str(capability_name).lower() == "total":
            # handle case when capability is "Total"
            return qs

        else:
            return qs.filter(capabilities=capability_name)


class NetCellDownload(InflowOutflowHelper):
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        # params required
        # 1. capability name
        # 2. start date
        # 3. end date
        # 4. attribute type (net)
        capability_name = kwargs.get("capability_name")
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

        qs_existing_bench = CapabilityFilter().get_qs_with_capability_filter(
            (
                self.get_qs_base_bench_facts(
                    start_date - timedelta(2)
                )
            ),
            capability_name
        )
        qs_project_ended_inflow = CapabilityFilter().get_qs_with_capability_filter(
            (
                self.get_qs_inflow_allocation_facts(
                    start_date - timedelta(1), end_date - timedelta(1)
                )
            ),
            capability_name
        )
        qs_project_started_outflow = CapabilityFilter().get_qs_with_capability_filter(
            (self.get_qs_outflow_allocation_facts(start_date, end_date)),
            capability_name
        )
        qs_exited_outflow = CapabilityFilter().get_qs_with_capability_filter(
            (self.get_qs_profile_facts(start_date, end_date)), capability_name
        )
        qs_training_ended_inflow = CapabilityFilter().get_qs_with_capability_filter(
            (
                self.get_qs_training_facts(
                    start_date - timedelta(1), end_date - timedelta(1)
                )
            ),
            capability_name,
        )

        qs_inflow = list(
            chain(qs_project_ended_inflow, qs_training_ended_inflow))

        qs_outflow = list(chain(qs_project_started_outflow, qs_exited_outflow))

        qs_inflow_union_existing_bench = list(
            chain(qs_existing_bench, qs_inflow))

        qs = []
        for element in qs_inflow_union_existing_bench:
            count = 0
            for item in qs_outflow:
                if element["email"] not in item["email"]:
                    count += 1
            if (count == len(qs_outflow)):
                qs.append(element)
        return qs


class ExistingBenchCellDownload(InflowOutflowHelper):
    # params required
    # 1. capability name
    # 2. start date
    # 3. end date
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        capability_name = kwargs.get("capability_name")
        start_date = kwargs.get("start_date")

        qs = self.get_qs_base_bench_facts(
            start_date - timedelta(2)
        )

        return CapabilityFilter().get_qs_with_capability_filter(qs, capability_name)


class OutflowCellDownload(InflowOutflowHelper):
    # params required
    # 1. capability name
    # 2. start date
    # 3. end date
    # 4. attribute type (project started outflow, exited outflow,net outflow)
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        capability_name = kwargs.get("capability_name")
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        attribute_type = kwargs.get("attribute_type")

        if attribute_type == "PROJECT_STARTED_OUTFLOW":
            return CapabilityFilter().get_qs_with_capability_filter(self.get_qs_outflow_allocation_facts(start_date, end_date), capability_name)

        elif attribute_type == "EXITED_OUTFLOW":
            return CapabilityFilter().get_qs_with_capability_filter(self.get_qs_profile_facts(start_date, end_date), capability_name)

        elif attribute_type == "OUTFLOW":
            qs_project_started_outflow = CapabilityFilter().get_qs_with_capability_filter(
                self.get_qs_outflow_allocation_facts(start_date, end_date), capability_name)

            qs_exited_outflow = CapabilityFilter().get_qs_with_capability_filter(
                self.get_qs_profile_facts(start_date, end_date), capability_name)

            qs = list(chain(qs_project_started_outflow, qs_exited_outflow))
            return qs


class InflowCellDownload(InflowOutflowHelper):
    # params required
    # 1. capability name
    # 2. start date
    # 3. end date
    # 4. attribute type (project ended inflow, training ended inflow, net inflow)
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        capability_name = kwargs.get("capability_name")
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        attribute_type = kwargs.get("attribute_type")

        if attribute_type == "PROJECT_ENDED_INFLOW":
            return CapabilityFilter().get_qs_with_capability_filter(self.get_qs_inflow_allocation_facts(start_date - timedelta(1), end_date - timedelta(1)), capability_name)

        elif attribute_type == "TRAINING_ENDED_INFLOW":
            return CapabilityFilter().get_qs_with_capability_filter(self.get_qs_training_facts(start_date - timedelta(1), end_date - timedelta(1)), capability_name)

        elif attribute_type == "INFLOW":
            qs_project_ended_inflow = CapabilityFilter().get_qs_with_capability_filter(
                self.get_qs_inflow_allocation_facts(start_date - timedelta(1), end_date - timedelta(1)), capability_name)
            qs_training_ended_inflow = CapabilityFilter().get_qs_with_capability_filter(
                self.get_qs_training_facts(start_date - timedelta(1), end_date - timedelta(1)), capability_name)

            qs = list(chain(qs_project_ended_inflow, qs_training_ended_inflow))
            return qs


class TotalHeadcountByBandCellDownload():
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        band_name = kwargs.get("band_name")
        compare_date = kwargs.get("compare_date")
        filters = kwargs.get("filters")

        qs = models.BaseBenchDataSource.objects.filter(Q(date_of_joining__lte=compare_date) & (
            Q(exit_date__gte=compare_date) | Q(exit_date__isnull=True))).distinct('email')
        if filters:
            qs = qs.filter(band__in=filters)

        if str(band_name).lower() == "total":
            # handle case when band is "Total"
            return qs

        else:
            if str(band_name) == "B8 (Intern)":
                return qs.filter(Q(band='B8') & Q(designation='Intern'))
            else:
                return qs.filter(band=band_name)


class TotalHeadcountFilters:

    def add_intern_filter(self, qs, band_name):

        if str(band_name) == "TBD":
            return qs.filter(Q(capabilities__isnull=True) & Q(band__in=["B8"]))

        elif str(band_name) == "Total":
            return qs.filter(Q(band__in=["B8"]))

        elif str(band_name) == "Other":
            return qs.filter(Q(capabilities=None) & Q(band__in=["B8"]))

        else:
            return qs.filter(Q(capabilities=band_name) & Q(band__in=["B8"]))

    def leadership_filter(self, qs, band_name):

        if str(band_name) == "Total":
            qs_leadership_filter = qs.filter(
                Q(capabilities__isnull=True) & Q(band__in=["B2", "B3H"]))
        else:
            qs_leadership_filter = qs.filter(
                Q(capabilities__isnull=True) & Q(band=band_name))
        return qs_leadership_filter

    def other_filter(self, qs, band_name):
        if str(band_name) == "Total":
            qs_other_filter = qs.filter(
                Q(capabilities=None) & ~Q(band__in=["B2", "B3H", "B8"]))
        else:
            qs_other_filter = qs.filter(
                Q(capabilities=None) & Q(band=band_name))
        return qs_other_filter

    def add_master_filter(self, qs, band_name, filters):
        # filters other then leadership, interns and other apply here
        qs_intermediate = qs.filter(capabilities__in=filters)

        if 'Leadership' in filters:
            qs_leadership = self.leadership_filter(qs, band_name)
            qs_intermediate = qs_intermediate | qs_leadership

        if 'Interns' in filters:
            qs_intern = self.add_intern_filter(qs, band_name)
            qs_intermediate = qs_intermediate | qs_intern

        if 'Other' in filters:
            qs_other = self.other_filter(qs, band_name)
            qs_intermediate = qs_intermediate | qs_other

        return qs_intermediate


class TotalHeadcountByCapabilityCellDownload(TotalHeadcountFilters):
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        band_name = kwargs.get("band_name")
        capability_name = kwargs.get("capability_name")
        compare_date = kwargs.get("compare_date")
        filters = kwargs.get("filters")

        qs = models.BaseBenchDataSource.objects.filter(Q(date_of_joining__lte=compare_date) & (
            Q(exit_date__gte=compare_date) | Q(exit_date__isnull=True))).distinct('email')
        if filters:
            qs = self.add_master_filter(qs, band_name, filters)

        if str(capability_name).lower() == "total":
            # handle case when capability is "Total"
            return qs

        elif str(capability_name) == "Leadership":

            return self.leadership_filter(qs, band_name)

        elif str(capability_name) == "Interns":
            return self.add_intern_filter(qs, band_name)

        elif str(capability_name) == "Other":
            return self.other_filter(qs, band_name)

        else:
            if str(band_name) == "Total":
                return qs.filter(Q(capabilities=capability_name))
            else:
                return qs.filter(Q(capabilities=capability_name) & Q(band=band_name))


class FilterHelper:

    qs = []
    # function to get queryset with filters added.

    def add_filter(self, filters, has_bands=False):

        if (has_bands):
            self.qs = models.TotalHeadcountByBand.objects.filter(
                band_name__in=filters)
        else:
            self.qs = models.TotalHeadcountByCapability.objects.filter(
                Q(capability_name__in=filters) & Q(band_name="Total"))

        self.qs = self.qs.values("duration").annotate(
            group=Count("duration")).order_by("duration")
        self.qs = self.qs.values("duration", "start_date", "compare_date", "category").annotate(group=Count('duration'), no_of_employees=Sum(
            "no_of_employees"), difference_of_employee_no=Sum("difference_of_employee_no"), count_percentage=Sum("count_percentage"))
        return self.qs


class TotalHeadcountByBandFilteredSummary(FilterHelper):
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        # filters variable takes band fields than needs to be shown
        filters = kwargs.get("filters")
        qs = self.add_filter(filters, True)
        for element in qs:
            element["band_name"] = "Total"
            element["count_percentage"] = round(element["count_percentage"], 2)
        return qs


class TotalHeadcountByCapabilityFilteredSummary(FilterHelper):
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        # filters variable takes capability fields than needs to be shown
        filters = kwargs.get("filters")
        qs = self.add_filter(filters)
        for element in qs:
            element["band_name"] = "Total"
            element["capability_name"] = "Total"
            element["count_percentage"] = round(element["count_percentage"], 2)
        return qs


class LastUpdateTimeResolver:
    def __call__(self, source, info, **kwargs):
        return models.LastUpdatedTime.objects.first()


class AllocatedHasherHelper:
    '''Helper for allocated hasher (cell download) '''

    def duration_filter_helper(self, duration, first_qs_row):

        if first_qs_row.week_1 == duration:
            qs_duration_filter = Q(employment_type_for_week_1='FTE')

        elif first_qs_row.week_2 == duration:
            qs_duration_filter = Q(employment_type_for_week_2='FTE')

        elif first_qs_row.week_3 == duration:
            qs_duration_filter = Q(employment_type_for_week_3='FTE')

        elif first_qs_row.week_4 == duration:
            qs_duration_filter = Q(employment_type_for_week_4='FTE')

        elif first_qs_row.week_5 == duration:
            qs_duration_filter = Q(employment_type_for_week_5='FTE')

        elif first_qs_row.week_6 == duration:
            qs_duration_filter = Q(employment_type_for_week_6='FTE')

        elif first_qs_row.week_7 == duration:
            qs_duration_filter = Q(employment_type_for_week_7='FTE')

        elif first_qs_row.week_8 == duration:
            qs_duration_filter = Q(employment_type_for_week_8='FTE')

        else:
            return False

        return qs_duration_filter

    def error_helper(self, error_message):
        return {"ok": False, "error": f"The error is -> {error_message}", "data": []}

    def cell_download_helper(self, pod_id, project_name, account_name, billability, duration, start_date=None, end_date=None, report_name=None):

        try:
            # calculating required dates
            now = date.today()
            current_monday = now - timedelta(days=now.weekday())
            current_week_sunday = now + timedelta(days=(6 - now.weekday()))

            if pod_id in ['Investment', 'Oversight Allocated']:
                qs_pod_id_filter = Q(derived_allocation_type=pod_id)
            else:
                qs_pod_id_filter = Q(pod_id=pod_id)

            # creating filter of project ,account name , billability and WBS code
            qs_project_filter = Q(project_name=project_name,
                                  account_name=account_name,
                                  derived_billability=billability) & ~Q(project_wbs_code__startswith='CED')

            # check report_name is present and return allocation filter accordingly
            if report_name == None:
                qs_allocation_filter = Q(
                    allocation_end_date__gte=current_monday, allocation_billing_start_date__lte=current_week_sunday)

            elif report_name == 'new_allocation':
                qs_allocation_filter = Q(
                    allocation_billing_start_date__range=(start_date, end_date))

            elif report_name == 'release':
                start_date = start_date - timedelta(days=7)
                end_date = end_date - timedelta(days=7)

                qs_allocation_filter = Q(
                    allocation_end_date__range=(start_date, end_date))
            else:
                return Exception("Incorrect report name")

            qs = models.UtilizationBaseFacts.objects.filter(
                qs_project_filter, qs_allocation_filter)

            # get first row of qs result
            first_qs_row = qs.first()

            # check if previous filter has data
            if first_qs_row is None:
                return Exception("Incorrect data requested")

            qs_duration_filter = self.duration_filter_helper(
                duration, first_qs_row)

            if qs_duration_filter == False:
                return Exception('Duration not acceptable')

            qs = qs.filter(qs_pod_id_filter,
                           qs_duration_filter).order_by('band')

            bands_count = {}
            final_result = []

            # merging final query result to list
            for element in qs.values():
                if element['band'] in bands_count:
                    bands_count[element['band']] += 1
                else:
                    bands_count[element['band']] = 1
                val = {}
                val['full_name'] = element['full_name']
                val['designation'] = element['designation']
                val['band'] = element['band']
                val['email'] = element['email']
                val['capability'] = element['capabilities']
                val['allocation_billing_start_date'] = str(
                    element['allocation_billing_start_date'])
                val['allocation_end_date'] = str(
                    element['allocation_end_date'])

                final_result.append(val)

            # adding band wise count
            final_result.append(bands_count)

            return final_result

        except Exception as e:
            raise GraphQLError(e)


class AccountWiseHeadcountAllocatedHasherCellDownload(AllocatedHasherHelper):
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):

        pod_id = kwargs.get("pod_id")
        project_name = kwargs.get("project_name")
        account_name = kwargs.get("account_name")
        billability = kwargs.get("billability")
        duration = kwargs.get('duration').strip()
        return self.cell_download_helper(pod_id, project_name, account_name, billability, duration)


class NestingHelper:
    # This function is created for reducing the complexity of main function group_data
    def group_data_helper(self, grouped_data, account_tuple, project_name, pod_id, pod_name, duration, allocations, bulge_ratio, billability):
        if account_tuple not in grouped_data:
            grouped_data[account_tuple] = {"children": {}}
        if project_name not in grouped_data[account_tuple]["children"]:
            grouped_data[account_tuple]["children"][project_name] = {
                "children": {}}
        if pod_id not in grouped_data[account_tuple]["children"][project_name]["children"]:
            grouped_data[account_tuple]["children"][project_name]["children"][pod_id] = {
                "pod_name": pod_name, "allocations": allocations, "billability": billability}
            if bulge_ratio is not None:
                grouped_data[account_tuple]["children"][project_name]["children"][pod_id]["bulge_ratio"] = bulge_ratio
        else:
            # read the duration from tuple
            if duration == account_tuple[0]:
                grouped_data[account_tuple]["children"][project_name]["children"][pod_id]["allocations"] += allocations

            if bulge_ratio is not None:
                grouped_data[account_tuple]["children"][project_name]["children"][pod_id]["bulge_ratio"] += bulge_ratio

     # Intermediate Grouped Data with account name at level 1, project name at level 2 and pod name at level 3
    def group_data(self, flat_data):
        grouped_data = {}
        for item in flat_data.values():
            account_name = item["account_name"]
            project_name = item["project_name"]
            pod_id = item["pod_id"]
            pod_name = item["pod_name"]
            duration = item["duration"]
            # To avoid problems of serialization, converting it to string
            start_date = str(item["start_date"])
            # To avoid problems of serialization, converting it to string
            end_date = str(item["end_date"])
            category = item["category"]
            billability = item["billability"]
            # To avoid problems of serialization, converting it to int
            allocations = int(item["allocations"])
            # To avoid float typecasting to NoneType
            if item.get("bulge_ratio") is not None:
                # To avoid problems of serialization, converting it to float
                # Retrieve bulge_ratio if available
                bulge_ratio = float(item.get("bulge_ratio"))
            else:
                bulge_ratio = item.get("bulge_ratio")
            # tuple of duration and account
            account_tuple = (duration, account_name)
            # To avoid sonarqube coginitive compelexity, shifting the code to group_data_helper function
            self.group_data_helper(grouped_data, account_tuple, project_name,
                                   pod_id, pod_name, duration, allocations, bulge_ratio, billability)

            grouped_data[account_tuple]["duration"] = duration
            grouped_data[account_tuple]["start_date"] = start_date
            grouped_data[account_tuple]["end_date"] = end_date
            grouped_data[account_tuple]["category"] = category
            grouped_data[account_tuple]["billability"] = billability
            if bulge_ratio is not None:
                grouped_data[account_tuple]["bulge_ratio"] = bulge_ratio

        return grouped_data
    # Calculating pods and total with their respective allocations, also adding bulge_ratio if it is present

    def process_pod_data(self, pod_id, pod_data, project_allocations, project_bulge_ratio):
        pod_allocations = pod_data.pop("allocations", None)
        pod_bulge_ratio = pod_data.pop("bulge_ratio", None)
        if pod_allocations is not None:
            pod_data["allocations"] = pod_allocations
        if pod_bulge_ratio is not None:
            pod_data["bulge_ratio"] = pod_bulge_ratio
        if pod_id == "Total":
            project_allocations = pod_allocations
            project_bulge_ratio = pod_bulge_ratio
        else:
            if project_allocations is None:
                project_allocations = pod_allocations
            if project_bulge_ratio is None:
                project_bulge_ratio = pod_bulge_ratio
            pod_data["allocations"] = pod_allocations
            pod_data["bulge_ratio"] = pod_bulge_ratio
        if pod_bulge_ratio is None:
            return project_allocations, None, {"pod_id": pod_id, "pod_name": pod_data["pod_name"], "allocations": pod_data["allocations"], "billability": pod_data["billability"]}
        else:
            return project_allocations, project_bulge_ratio, {"pod_id": pod_id, "pod_name": pod_data["pod_name"], "allocations": pod_data["allocations"], "bulge_ratio": pod_data.get("bulge_ratio"), "billability": pod_data["billability"]}

    # Adding Pods Data in Project Level with their respective allocations, also bulge_ratio if it is present

    def process_project_data(self, project_name, project_data):
        project_allocations = None
        project_bulge_ratio = None
        project_pods_type = []
        for pod_id, pod_data in project_data["children"].items():
            project_allocations, project_bulge_ratio, pod_info = self.process_pod_data(
                pod_id, pod_data, project_allocations, project_bulge_ratio)
            project_pods_type.append(pod_info)
        if project_allocations is None:
            project_allocations = project_data.get("allocations")
        if project_bulge_ratio is None:
            project_bulge_ratio = project_data.get("bulge_ratio")
        if project_bulge_ratio is None:
            return {"project_name": project_name, "children": project_pods_type, "allocations": project_allocations}
        else:
            return {"project_name": project_name, "children": project_pods_type, "allocations": project_allocations, "bulge_ratio": project_bulge_ratio}

    # Adding Project Data in Account Level with their respective allocations, also bulge_ratio if it is present
    def process_account_data(self, account_tuple, account_data):
        account_allocations = None
        account_bulge_ratio = None
        account_duration = account_data.get("duration")
        account_start_date = account_data.get("start_date")
        account_end_date = account_data.get("end_date")
        account_category = account_data.get("category")
        account_billability = account_data.get("billability")
        account_projects = []
        for project_name, project_data in account_data["children"].items():
            project_data_processed = self.process_project_data(
                project_name, project_data)
            account_projects.append(project_data_processed)
            if project_name == "Total":
                account_allocations = project_data_processed["allocations"]
                account_bulge_ratio = project_data_processed.get(
                    "bulge_ratio")  # Retrieve bulge_ratio if available
        if account_allocations is None:
            account_allocations = account_data.get("allocations")
        if account_bulge_ratio is None:
            return {"account_name": account_tuple[1], "children": account_projects, "duration": account_duration, "start_date": account_start_date, "end_date": account_end_date, "category": account_category, "billability": account_billability, "allocations": account_allocations}
        else:
            return {"account_name": account_tuple[1], "children": account_projects, "duration": account_duration, "start_date": account_start_date, "end_date": account_end_date, "category": account_category, "billability": account_billability, "allocations": account_allocations, "bulge_ratio": account_bulge_ratio}


class NestingOfAccountWiseHeadcount(NestingHelper):
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):

        # Adding Master Filter for billability and Multiple Account Names
        billability = kwargs.get("billability")
        filters = kwargs.get("filters")
        flat_data = models.AccountWiseHeadcount.objects.all()

        if billability:
            flat_data = flat_data.filter(billability__in=billability)

        if filters:
            flat_data = flat_data.filter(account_name__in=filters)

        # Making a Intermediate Grouped Data for our required Nested API
        grouped_data = self.group_data(flat_data)

        # Nested API
        nested_data = []
        for account_tuple, account_data in grouped_data.items():
            nested_data.append(self.process_account_data(
                account_tuple, account_data))

        return nested_data


class GetPodDetailsResolver():
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        pod_id = kwargs.get("id")
        qs = b2b_get_pod_details_service(pod_id)
        qs = qs.json()
        return qs.get("data").get("podDetails")


class NestingOfNewAllocationsFacts(NestingHelper):
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        # Adding Master Filter for billability and Multiple Account Names
        billability = kwargs.get("billability")
        filters = kwargs.get("filters")
        flat_data = models.NewAllocationsFacts.objects.all()
        flat_data = flat_data.filter(billability=billability)
        if filters:
            flat_data = flat_data.filter(account_name__in=filters)

        # Making a Intermediate Grouped Data for our required Nested API
        grouped_data = self.group_data(flat_data)

        # Nested API
        nested_data = []
        for account_tuple, account_data in grouped_data.items():
            nested_data.append(self.process_account_data(
                account_tuple, account_data))

        return nested_data


class NestingOfReleasesFacts(NestingHelper):
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        # Adding Master Filter for billability and Multiple Account Names
        billability = kwargs.get("billability")
        filters = kwargs.get("filters")
        flat_data = models.ReleasesFacts.objects.all()
        flat_data = flat_data.filter(billability=billability)
        if filters:
            flat_data = flat_data.filter(account_name__in=filters)

        # Making a Intermediate Grouped Data for our required Nested API
        grouped_data = self.group_data(flat_data)

        # Nested API
        nested_data = []
        for account_tuple, account_data in grouped_data.items():
            nested_data.append(self.process_account_data(
                account_tuple, account_data))

        return nested_data


class NewAllocationAndReleaseAllocatedHasherCellDownload(AllocatedHasherHelper):
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):

        pod_id = kwargs.get("pod_id")
        project_name = kwargs.get("project_name")
        account_name = kwargs.get("account_name")
        billability = kwargs.get("billability")
        duration = kwargs.get('duration').strip()
        report_name = kwargs.get('report_name').strip().lower()
        start_date = kwargs.get('start_date')
        end_date = kwargs.get('end_date')

        return self.cell_download_helper(pod_id, project_name, account_name, billability, duration, start_date, end_date, report_name)


class OverallFreePoolByBandFilteredSummary():
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        # filters variable takes band fields than needs to be shown
        filters = kwargs.get("filters")
        qs = []
        if (len(filters) == 0):
            qs = models.OverallBenchByBand.objects.all()
        else:
            qs_filtered = models.OverallBenchByBand.objects.filter(
                band_name__in=filters)
            qs_sum = qs_filtered.aggregate(bench=Sum('bench'), lop=Sum('lop'), resigned=Sum('resigned'), maternity=Sum('maternity'), allocation_pending=Sum('allocation_pending'), nominated=Sum('nominated'), non_deployable_total=Sum(
                'non_deployable_total'), deployable_total=Sum('deployable_total'), sabbatical=Sum('sabbatical'), grand_total=Sum('grand_total'), grand_total_percentage=Sum('grand_total_percentage'))
            qs_dummy = {"id": "Total", "band_name": 'Total'}
            qs_dummy.update(qs_sum)
            qs = list(chain(qs_filtered, [qs_dummy]))
        return qs


class OverallFreePoolByCapabilityFilteredSummary():
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        # filters variable takes band fields than needs to be shown
        filters = kwargs.get("filters")
        qs = []
        if (len(filters) == 0):
            qs = models.OverallBenchByCapability.objects.all()
        else:
            qs_filtered = models.OverallBenchByCapability.objects.filter(
                capability_name__in=filters)
            qs_sum = qs_filtered.filter(band_name='Total').aggregate(bench=Sum('bench'), lop=Sum('lop'), resigned=Sum('resigned'), maternity=Sum('maternity'), allocation_pending=Sum('allocation_pending'), nominated=Sum(
                'nominated'), non_deployable_total=Sum('non_deployable_total'), deployable_total=Sum('deployable_total'), sabbatical=Sum('sabbatical'), grand_total=Sum('grand_total'), grand_total_percentage=Sum('grand_total_percentage'))
            qs_dummy = {"id": "Total",
                        "capability_name": 'Total', "band_name": 'Total'}
            qs_dummy.update(qs_sum)
            qs = list(chain(qs_filtered, [qs_dummy]))
        return qs


class PeopleOnBenchHelper():

    def __init__(self):
        today = date.today()
        # filter added
        self.qs = models.BaseBenchDataSource.objects.filter((Q(on_bench__iexact='true') | Q(
            on_bench__iexact='n/a')) & Q(department__iexact='delivery') & Q(date_of_joining__lte=today) & Q(band__in=['B5H', 'B5L', 'B6H', 'B6L', 'B7', 'B8']))

    def add_band_filter(self, band_name):
        if 'B8 (Intern)' in band_name:
            if (len(band_name) == 1):
                return self.qs.filter((Q(band='B8') & Q(designation='Intern')))
            else:
                qs_intern = self.qs.filter(
                    (Q(band='B8') & Q(designation='Intern')))
                qs_rest = self.qs.filter(
                    Q(band__in=band_name) & ~Q(designation='Intern'))
                return qs_intern | qs_rest
        elif 'Total' in band_name:
            return self.qs
        else:
            return self.qs.filter(Q(band__in=band_name) & ~Q(designation='Intern'))

    def add_capability_filter(self, capability_name, band_name):
        if 'Total' in capability_name:
            return self.qs
        elif 'Interns' in capability_name and (len(capability_name) == 1):
            return self.add_intern_filter(band_name)
        elif 'Other' in capability_name and (len(capability_name) == 1):
            return self.add_other_capability_filter(band_name)
        elif (len(capability_name) == 1):
            return self.add_capability_name_filter(band_name, capability_name)
        else:
            return self.add_multilpe_filters(capability_name)

    def add_multilpe_filters(self, capability_name):
        qs_rest = self.qs.filter(
            Q(capabilities__in=capability_name) & ~Q(designation='Intern'))
        if 'Interns' in capability_name:
            capability_name.remove('Interns')
            qs = self.add_intern_filter('Total')
            qs_rest = qs_rest | qs
        if 'Other' in capability_name:
            capability_name.remove('Other')
            qs = self.add_other_capability_filter('Total')
            qs_rest = qs_rest | qs
        if (len(capability_name) > 0):
            qs = self.add_capability_name_filter('Total', capability_name)
            qs_rest = qs_rest | qs
        return qs_rest

    def add_capability_name_filter(self, band_name, capability_name):
        if band_name == 'Total':
            return self.qs.filter(Q(capabilities__in=capability_name) & ~Q(designation='Intern'))
        else:
            return self.qs.filter(Q(capabilities__in=capability_name) & ~Q(designation='Intern') & Q(band=band_name))

    def add_intern_filter(self, band_name):
        if band_name == 'Total':
            return self.qs.filter(Q(band="B8") & Q(designation='Intern'))
        elif band_name == "TBD":
            return self.qs.filter(Q(band="B8") & Q(designation='Intern') & Q(capabilities__isnull=True))
        elif band_name == "Other":
            return self.qs.filter(Q(band="B8") & Q(designation='Intern') & ~Q(capabilities__in=self.get_base_capability_list()) & Q(capabilities__isnull=False))
        else:
            return self.qs.filter(Q(band="B8") & Q(designation='Intern') & Q(capabilities=band_name))

    def add_other_capability_filter(self, band_name):
        if band_name == 'Total':
            return self.qs.filter((Q(capabilities__isnull=True) | ~Q(capabilities__in=self.get_base_capability_list())) & ~Q(designation='Intern') & ~Q(band__in=["B2", "B3H"]))
        else:
            return self.qs.filter(Q(band=band_name) & ~Q(designation='Intern') & (Q(capabilities__isnull=True) | ~Q(capabilities__in=self.get_base_capability_list())))

    def get_base_capability_list(self):
        capability_list = BaseCapabilities().get_base_capabilities()
        return capability_list


class OverallFreePoolHelper():

    def __init__(self, qs):
        self.qs = qs

    def redirect_columns(self, column):
        if column in ['lop', 'maternity', 'resigned', 'sabbatical', 'allocationPending', 'nominated']:
            return self.add_filter(column)
        elif str(column).lower() == 'nondeployabletotal':
            return self.get_non_deployable_total_column()
        elif str(column).lower() == 'bench':
            return self.get_bench_column()
        elif str(column).lower() == 'deployabletotal':
            return self.get_deployable_total_column()
        elif str(column).lower() == 'grandtotal':
            return self.get_grand_total_column()

    def add_filter(self, column):

        if str(column).lower() == 'lop':
            return self.qs.filter(leave_type__iexact='loss of pay')
        elif str(column).lower() == 'resigned':
            return self.qs.filter(resigned_on__isnull=False)
        elif str(column).lower() == 'maternity':
            return self.qs.filter(leave_type__iexact='maternity')
        elif str(column).lower() == 'sabbatical':
            return self.qs.filter(leave_type__iexact='sabbatical')
        elif str(column).lower() == 'allocationpending':
            return self.qs.filter(nomination_status__iexact='selected')
        elif str(column).lower() == 'nominated':
            return self.qs.filter(Q(nomination_status__iexact='nominated') | Q(nomination_status__iexact='considered'))

    def get_non_deployable_total_column(self):

        return list(chain(self.add_filter('lop'), self.add_filter('resigned'), self.add_filter('maternity'), self.add_filter('sabbatical'), self.add_filter('allocationpending')))

    def get_bench_column(self):

        qs_chained = list(
            chain(self.get_non_deployable_total_column(), self.add_filter('nominated')))

        qs = []
        for element in self.qs:
            count = 0
            for item in qs_chained:
                if getattr(element, "email") not in getattr(item, "email"):
                    count += 1
            if (count == len(qs_chained)):
                qs.append(element)
        return qs

    def get_deployable_total_column(self):

        return list(chain(self.get_bench_column(), self.add_filter('nominated')))

    def get_grand_total_column(self):

        return list(chain(self.get_deployable_total_column(), self.get_non_deployable_total_column()))


class OverallFreePoolByBandCellDownload():
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        band_name = kwargs.get("bands")
        column = kwargs.get("column")

        qs = PeopleOnBenchHelper().add_band_filter(band_name)
        return OverallFreePoolHelper(qs).redirect_columns(column)


class OverallFreePoolByCapabilityCellDownload():
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        band_name = kwargs.get("band")
        capability_name = kwargs.get("capabilities")
        column = kwargs.get("column")

        qs = PeopleOnBenchHelper().add_capability_filter(capability_name, band_name)
        return OverallFreePoolHelper(qs).redirect_columns(column)


class LastApprovedResolver:
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        return core_models.LastApproved.objects.last()


class UtilizationSnapshotReportResolver:
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):

        # get current period name
        current_period = models.Periods.objects.filter(
            period_tag="current_period").values()[0]

        current_period_name = current_period["period_name"]
        current_period_start_date = current_period["period_start_date"]

        qs = core_models.UtilizationFactBackup.objects.all()

        qs_result = {}

        # going over all the data of backup
        for val in qs.values():
            category = "Previous Period"

            if current_period_name == val["period_name"]:

                # to check if data is of current period
                if val["start_date"] < current_period_start_date:
                    continue
                category = "Current Period"

            # going over snapshot data of every row in backup table.
            for snap_data in json.loads(val["snapshot_data"]):
                # key to seperate different billing attribute and billing attribute type
                key = snap_data["billing_attribute"] + \
                    "_" + snap_data["billing_attribute_type"]

                data_value = {
                    "period_name": val["period_name"],
                    "week_name": val["week_name"],
                    "start_date": str(val["start_date"]),
                    "end_date": str(val["end_date"]),
                    "record_type": val["record_type"],
                    "category": category,
                    "billing_attribute": snap_data["billing_attribute"],
                    "billing_attribute_type": snap_data["billing_attribute_type"],
                    "fte_count": snap_data["fte_count"],
                    "fte_percentage": snap_data["fte_percent"]
                }

                if key not in qs_result:
                    qs_result[key] = {"billing_attribute": snap_data["billing_attribute"],
                                      "billing_attribute_type": snap_data["billing_attribute_type"], "value": []}

                qs_result[key]["value"].append(data_value)

        final_result = []

        data_id = 1
        # adding value to list of objects
        for data in qs_result.values():
            data["id"] = data_id
            data_id = data_id + 1
            final_result.append(data)

        return final_result


class UserManualResolver:
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        report_name = kwargs.get("report_name")
        qs = core_models.UsersManual.objects.all()
        qs = qs.filter(report_name=report_name)
        return qs

class AccountWiseHeadcountSnapshotReportResolver:

    def merge_allocations_count(self, value_list):

        value_dict = dict()
        for value in value_list:
            key = value["period_name"]+value["week_name"]+value["start_date"] + \
                value["end_date"]+value["record_type"]+value["category"]

            if value_dict.get(key) is None:
                value_dict[key] = value
            else:
                item = value_dict[key]
                item['allocations'] = int(
                    item['allocations']) + int(value['allocations'])

        final_value_list = []

        for value in value_dict.values():
            final_value_list.append(value)

        return final_value_list

    def snapshot_helper(self, billability, snapshot):

        snapshot_result = []

        # if billability is not empty
        if billability:
            # apply billability filter
            for billability_val in billability:
                snapshot_result.extend(snapshot[billability_val])
        else:
            # show all the billability
            for snapshot_values in snapshot.values():
                snapshot_result.extend(snapshot_values)

        return snapshot_result

    def grand_total_calculator(self, qs_result):

        # function to calculate grand_total.
        hashmap = dict()
        for qs in qs_result.values():
            # merge allocations when have multiple billability in a account
            qs['value'] = self.merge_allocations_count(qs['value'])

            if qs["account_name"] == 'Total':
                qs_dummy = qs["value"]
                for item in qs_dummy:
                    string = item["period_name"]+" "+item["week_name"]+" "+item["start_date"] + \
                        " "+item["end_date"]+" " + \
                        item["record_type"]+" "+item["category"]
                    if hashmap.get(string) is not None:
                        hashmap[string] = int(
                            item["allocations"]) + int(hashmap.get(string))
                    else:
                        hashmap[string] = int(item["allocations"])

        return hashmap

    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):

        billability = kwargs.get("billability")

        # get current period
        current_period = models.Periods.objects.filter(
            period_tag="current_period").values()[0]

        current_period_name = current_period["period_name"]
        current_period_start_date = current_period["period_start_date"]

        qs = core_models.AccountWiseHeadcountSnapshot.objects.all()

        qs_result = {}

        # going over all the data of backup
        for val in qs.values():
            category = "Previous Period"

            if current_period_name == val["period_name"]:

                # to check if data is of current period
                if val["start_date"] < current_period_start_date:
                    continue
                category = "Current Period"

            snapshot = json.loads(val["snapshot_data"])

            snapshot_data = self.snapshot_helper(billability, snapshot)

            # going over snapshot data of every row in backup table.
            for snap_val in snapshot_data:
                # key to seperate different account_name and project_name type
                key = snap_val["account_name"] + \
                    "_" + snap_val["project_name"]

                data_value = {
                    "period_name": val["period_name"],
                    "week_name": val["week_name"],
                    "start_date": str(val["start_date"]),
                    "end_date": str(val["end_date"]),
                    "record_type": val["record_type"],
                    "category": category,
                    "account_name": snap_val["account_name"],
                    "project_name": snap_val["project_name"],
                    "billability": snap_val["billability"],
                    "allocations": snap_val["allocations"]
                }

                if key not in qs_result:
                    qs_result[key] = {"account_name": snap_val["account_name"],
                                      "project_name": snap_val["project_name"], "value": []}

                qs_result[key]["value"].append(data_value)

        if len(qs_result) > 0:
            hashmap = self.grand_total_calculator(qs_result)

            grand_total = {
                "Total_Total": {
                    "account_name": "Total",
                    "project_name": "Total",
                    "value": []
                }
            }

            for item in hashmap:
                total_result = {
                    "period_name": item.rsplit(" ")[0],
                    "week_name": item.rsplit(" ")[1],
                    "start_date": item.rsplit(" ")[2],
                    "end_date": item.rsplit(" ")[3],
                    "record_type": item.rsplit(" ")[4]+" "+item.rsplit(" ")[5],
                    "category": item.rsplit(" ")[6]+" "+item.rsplit(" ")[7],
                    "account_name": "Total",
                    "project_name": "Total",
                    "billability": "Total",
                    "allocations": hashmap[item]
                }
                grand_total["Total_Total"]["value"].append(total_result)

            qs_result.update(grand_total)

        final_result = []
        data_id = 1
        # adding value to list of objects
        for data in qs_result.values():
            data["id"] = data_id
            data_id = data_id + 1
            final_result.append(data)

        return final_result

class DemandBaseDataForPostionWise(DjangoListResolver):
    @classmethod
    @check_permissions
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
    
        qs_value = models.DemandBaseDataSource.objects.all().distinct('position_config_id')
        return qs_value
    
class WbsReportNestingHelper:
    def group_data(self, flat_data):
        grouped_data = {}
        for item in flat_data.values():
            account_name = item["account_name"]
            project_name = item["project_name"]
            pod_id = item["pod_id"]
            pod_name = item["pod_name"]
            billability = item["billability"]
            active_pod_wbs_codes = item["active_pod_wbs_codes"]
            active_project_wbs_code = item["active_project_wbs_code"]

            if account_name not in grouped_data:
                grouped_data[account_name] = {"children": {}}
            if project_name not in grouped_data[account_name]["children"]:
                grouped_data[account_name]["children"][project_name] = {
                    "children": {}, "active_project_wbs_code":active_project_wbs_code}
            if pod_id not in grouped_data[account_name]["children"][project_name]["children"] and pod_id != "Total":
                grouped_data[account_name]["children"][project_name]["children"][pod_id] = {
                    "pod_name": pod_name, "active_pod_wbs_codes":active_pod_wbs_codes, "billability":billability}
                
        return grouped_data
    
    def process_pod_data(self, pod_id, pod_data):
        active_pod_wbs_codes_types = pod_data.pop("active_pod_wbs_codes", None)
        return {"pod_id": pod_id, "pod_name": pod_data["pod_name"], "billability": pod_data["billability"], "active_pod_wbs_codes": active_pod_wbs_codes_types}

    def process_project_data(self, project_name, project_data):
        project_pods_type = []
        
        for pod_id, pod_data in project_data["children"].items():
            pod_info = self.process_pod_data(
                pod_id, pod_data)
            project_pods_type.append(pod_info)
        pods_in_project = len(project_pods_type)
        return pods_in_project , {"project_name": project_name, "no_of_pods": pods_in_project, "active_project_wbs_code": project_data["active_project_wbs_code"], "children": project_pods_type}
        
    def process_account_data(self, account_name, account_data):
        account_projects = []
        total_pods = 0
        for project_name, project_data in account_data["children"].items():
            pods_in_project, project_data_processed = self.process_project_data(
                project_name, project_data)
            total_pods = total_pods + pods_in_project
            account_projects.append(project_data_processed)
        
        return {"account_name": account_name, "no_of_projects": len(account_projects), "total_pods": total_pods, "children": account_projects}
    

class NestingOfWbsTrackingReport(WbsReportNestingHelper):
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):

        # Adding Master Filter for billability and Multiple Account Names
        billability = kwargs.get("billability")
        filters = kwargs.get("filters")
        flat_data = models.WbsTrackingFact.objects.all()

        if billability:
            flat_data = flat_data.filter(billability__in=billability)

        if filters:
            flat_data = flat_data.filter(account_name__in=filters)

        # Making a Intermediate Grouped Data for our required Nested API
        grouped_data = self.group_data(flat_data)

        # Nested API
        nested_data = []
        for account_name, account_data in grouped_data.items():
            nested_data.append(self.process_account_data(
                account_name, account_data))

        return nested_data
    
class WBSTrackerAccountsResolver:

    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        qs = models.WbsTrackingFact.objects.all().values('account_name',
                                                         'billability').distinct('account_name').filter(~Q(billability='Total'))
        return list(qs)


class BillingWBSCodePopUpResolver:

    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        try:
            pod_id = kwargs.get("pod_id")
            qs = models.WbsTrackingFact.objects.filter(
                pod_id=pod_id).values_list('pod_id', 'active_pod_wbs_codes')
            result = {"pod_id": pod_id}
            children = []
            for item in list(qs)[0][1]:
                value = {}
                value["wbs_code"] = item["wbs_code"]
                value["percentage"] = item["allocation_amount_percentage"]
                children.append(value)
            result["active_pod_details"] = children

            return result

        except Exception:
            raise PodIdNotFound("POD-ID does not exist")


class DistinctFieldResolver:
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        try:
            model_name = kwargs.get("model_name")
            columns_list = kwargs.get("column_list")
            final_result = {}
            model_instance = getattr(models, model_name)

            for column in columns_list:

                field_data = model_instance.objects.values_list(
                    column, flat=True).distinct().order_by(column)
                final_result[column] = list(field_data)

            data = json.loads(json.dumps(final_result, cls=DjangoJSONEncoder))
            response_data = {"ok": True, "error": "", "data": data}

            return response_data

        except (AttributeError, FieldError) as e:
            return Exception(e)


class GetAllFieldsOfModelResolver:
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):
        try:
            model_name = kwargs.get("model_name")

            model_instance = getattr(models, model_name)

            list_of_columns = model_instance._meta.get_fields()

            response = [{"field_name": []}]

            for values in list_of_columns:
                response[0]['field_name'].append(values.name)

            return response
        except (AttributeError):
            return Exception("Model does not exists")

class ReportTemplateHelper:
    def apply_selected_column(self,column_list):

        qs = models.MasterBaseFact.objects.values(*column_list)

        return qs

    def apply_filter_on_column(self, qs, filter_list):

        if filter_list == None:
            return qs

        for filters in filter_list :
            column_name = filters.get("column_name",None)
            if column_name :
                contain_only = filters.get("contains_only", [])

                if contain_only:
                    qs = qs.filter(**{f"{column_name}"+"__in": contain_only})

        return qs

    def apply_template(self,info, template_id):
        
        template = core_models.ReportsTemplate.objects.filter(template_id= template_id, email= info.context.user.email).first()

        if template == None:
            return None

        qs = self.apply_selected_column(template.selected_columns)

        qs = self.apply_filter_on_column(qs,template.column_filters)

        return qs 

    def result_serializers(self,qs):
        qs_list = list(qs)

        return json.loads(json.dumps(qs_list, cls=DjangoJSONEncoder))

class ApplyReportTeamplateResolver(ReportTemplateHelper):
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):

        template_id = kwargs.get("template_id")

        qs = self.apply_template(info , template_id)

        if qs == None:
            return Exception("Template does not exist Or Template does not belongs to you")

        final_result = self.result_serializers(qs)

        return {"ok": True, "error": "", "data": final_result}

class GenerateReportResolver(ReportTemplateHelper):
    @custom_resolver_check_perm
    def __call__(self, source, info, **kwargs):

        selected_columns = kwargs.get("selected_columns", [])
        column_filters = kwargs.get("column_filters", [])

        qs = self.apply_selected_column(selected_columns)

        qs = self.apply_filter_on_column(qs,column_filters)

        qs_list = list(qs)

        final_result = self.result_serializers(qs)

        return {"ok": True, "error": "", "data": final_result}
