import React, { useState, useEffect, useMemo } from "react";
import { useLazyQuery, useQuery } from "@apollo/client";
import {
  GET_BENCH_AGING_BY_BAND,
  GET_BENCH_AGING_BY_CAPABILITY,
  GET_BENCH_AGING_BAND_WISE_SPLIT,
  GET_BENCH_AGING_DETAILS_BY_CAP_OR_BAND,
  GET_BENCH_AGING_DETAILS_BY_TOTAL_CAPABILITY,
  GET_BENCH_AGING_DETAILS_BY_TOTAL_BAND,
  GET_BENCH_AGING_DETAILS_BY_BAND,
} from "../../services/Queries";
import {
  Error,
  CAPABILITY,
  BAND,
  FREE_POOL_AGEING_TITLE,
  benchAgingLabels,
  benchAgingLabelsTitle,
  benchAgingDaysTitle,
  benchAgingDaysOrder,
  benchAgingDownloadTitle,
  BY_CAPABILITY,
  BY_BAND,
} from "../../lib/Constants";
import ExportToExcel from "../ExportToExcel/ExportToExcel";
import UserManual from "../UserManual/UserManual";
import { Table, Radio, Result, Button } from "antd";
import {
  LeftOutlined,
  RightOutlined,
  DownloadOutlined,
} from "@ant-design/icons";
import { isFixed, splitDate, downloadCellData } from "../Widgets/Utils";
import { getRecordType, getTitle } from "./Utils";
import "./BenchAging.scss";

export default function BenchAging() {
  //Initializations

  //Stores the type of query. Default query type -- Capability
  const [benchType, setBenchType] = useState(CAPABILITY);
  //Stores query data for data fetching.
  const [dataSource, setDataSource] = useState([]);
  // Stores the data for table source
  const [exportDataSource, setExportDataSource] = useState([]);
  //It Stores the table column
  const [colArray, setColArray] = useState([]);
  // Stores all the columns for table export
  const [exportColArray, setExportColArray] = useState([]);
  // Stores total field for table summary
  const [tableTotal, setTableTotal] = useState({});
  //It stores the state of column.(For expanding and collapsing)
  const [hiddenTitle, setHiddentitle] = useState({
    allocationPendingEmployees: true,
    nominatedEmployees: true,
    availableEmployees: true,
  });
  // stores band wise data based on capability
  const [bandSplitData, setBandSplitData] = useState([]);
  //It stores the status of the flag.
  const [flag, setFlag] = useState(false);

  const [fileName, setFileName] = useState("");
  const [errorMessage, setErrorMessage] = useState("");

  // Stores height of the header containing table title and the export button
  let pageHeaderHeight = document.getElementById("benchHeader")?.clientHeight;

  // Toggle for radio button
  const onToggle = (e) => {
    setBenchType(e.target.value);
  };
  //Stores the type of the query
  const type = getRecordType(benchType);

  //It stores the state and collapse the column.
  const collapseColumn = (benchAgingLabels) => {
    let collapse = hiddenTitle;
    collapse[benchAgingLabels] = !collapse[benchAgingLabels];
    setHiddentitle(collapse);
    setFlag(!flag);
  };

  const DownloadFunctionBand = (
    name,
    status,
    duration,
    isChild,
    startDate,
    endDate
  ) => {
    /*Calling TotalByBandDetails Query when clicking on the total column.
              Passing Parameters (bandName, title)*/
    if (duration === "ALL_DURATION" && !isChild) {
      setFileName(name + "-" + status + "-" + duration);
      benchAgingTotalByBand({
        variables: { bandName: name, benchType: status },
      });
      return;
    }

    /*Calling BandDetails Query and Passing parameters (BandName,startDate, endDate, title)*/
    if (status !== "TOTAL_BENCH") {
      setFileName(name + "-" + status + "-" + duration);
      benchAgingBandDetails({
        variables: {
          bandName: name,
          startDate: startDate,
          endDate: endDate,
          benchType: status,
        },
      });
      return;
    }

    //Calling TotalByBandDetails Query and Passing parameters (BandName, title).
    if (status === "TOTAL_BENCH") {
      setFileName(name + "-" + status);
      benchAgingTotalByBand({
        variables: { bandName: name, benchType: status },
      });
    }
  };

  const DownloadFunctionCapability = (
    name,
    status,
    capability,
    duration,
    start,
    end,
    isChild
  ) => {
    /*Calling CapabilityandBandDetails Query when the only days wise information is clicked.
              Passing Parameters (capabilityName, startDate, endDate, title)*/
    if (status !== "TOTAL_BENCH" && !isChild && duration !== "ALL_DURATION") {
      setFileName(name + "-" + status + "-" + duration);
      benchAgingCapAndBandDetails({
        variables: {
          capabilityName: name,
          bandName: "",
          startDate: start,
          endDate: end,
          benchType: status,
        },
      });
      return;
    }

    /*Calling CapabilityandBandDetails Query when the only row expansion cell column is clicked.
              Passing Parameters (capabilityName, bandName, startDate, endDate, title)*/
    if (duration === "ALL_DURATION" && isChild) {
      setFileName(capability + "-" + name + "-" + status + "-" + duration);
      benchAgingCapAndBandDetails({
        variables: {
          capabilityName: capability,
          bandName: name,
          startDate: "1990-01-01",
          endDate: "2070-01-01",
          benchType: status,
        },
      });
      return;
    }

    /*Calling TotalByCapabilityDetails Query when clicking on the total column.
              Passing Parameters (capabilityName, title)*/
    if (duration === "ALL_DURATION" && !isChild) {
      setFileName(name + "-" + status + "-" + duration);
      benchAgingTotalByCap({
        variables: { capabilityName: name, benchType: status },
      });
      return;
    }

    /*Calling CapabilityandBandDetails Query when the row and column is expanded.
              Passing parameters (capabilityName, bandName, startDate, endDate, title) */
    if (status !== "TOTAL_BENCH" && isChild && duration !== "ALL_DURATION") {
      setFileName(capability + "-" + name + "-" + status + "-" + duration);
      benchAgingCapAndBandDetails({
        variables: {
          capabilityName: capability,
          bandName: name,
          startDate: start,
          endDate: end,
          benchType: status,
        },
      });
      return;
    }

    /*Calling TotalByCapabilityDetails Query. For Grand_Total 
              Passing parameters (capabilityName, title)*/
    if (status === "TOTAL_BENCH" && !isChild) {
      setFileName(name + "-" + status);
      benchAgingTotalByCap({
        variables: { capabilityName: name, benchType: status },
      });
      return;
    }

    /*Calling TotalByCapabilityDetails Query. For Grand_Total when using row expansion functionality.
              Passing parameters (capabilityName, bandName, title, start_date, end_date)*/
    if (status === "TOTAL_BENCH" && isChild) {
      setFileName(capability + "-" + name + "-" + status);
      benchAgingCapAndBandDetails({
        variables: {
          capabilityName: capability,
          bandName: name,
          startDate: "1990-01-01",
          endDate: "2070-01-01",
          benchType: status,
        },
      });
    }
  };

  /**
   * Download function to get the data after clicking the particular cell.
   *
   * @param {boolean} isChild To check the expansion functionality.
   * @param {string} name It defines the capability name.
   * @param {string} capability It gives us the parent capability for bandWise distribution.
   * @param {string} startDate StartDate of the selected Column.
   * @param {string} duration It gives  the days in which the cell is in.
   * @param {string} endDate It shows the endDate.
   * @param {string} status It gives  the title(Allocation Pending, Nominated, Bench)
   */
  const functiondownload = (
    isChild,
    name,
    capability,
    startDate,
    duration,
    endDate,
    status
  ) => {
    let start = startDate;
    let end = endDate;
    if (duration === "ALL_DURATION") {
      start = "";
      end = "";
    }

    if (type === "capability") {
      DownloadFunctionCapability(
        name,
        status,
        capability,
        duration,
        start,
        end,
        isChild
      );
    } else {
      DownloadFunctionBand(name, status, duration, isChild, startDate, endDate);
    }
  };

  // Sorting on the basis of Order of duration ["MORE_THAN_90_DAYS","BETWEEN_61_AND_90_DAYS",
  // "BETWEEN_31_AND_60_DAYS", "LESS_THAN_30_DAYS","ALL_DURATION"].
  const sortByDuration = (data) => {
    return data.sort(function (a, b) {
      if (
        benchAgingDaysOrder.indexOf(a.duration) >
        benchAgingDaysOrder.indexOf(b.duration)
      )
        return 1;
      else return -1;
    });
  };

  /**
   * Fetch column title based on bench status
   * i.e.   "allocationPendingEmployees","nominatedEmployees","availableEmployees"
   *
   * @param {string} label Bench status of the data
   * @param {boolean} includeJSX To include or exclude button in columns
   * @returns Title of the parent column
   */
  const getColumnTitle = (label, includeJSX) => {
    if (includeJSX)
      return (
        <div className="titleSection">
          <div className="titleSectionLabel">
            {benchAgingLabelsTitle[label]}
          </div>
          <Button
            data-testid="columnExpandButton"
            className="controlButton"
            onClick={() => collapseColumn(label)}
            icon={
              !hiddenTitle[label] ? (
                <LeftOutlined className="controlButtonIcon" />
              ) : (
                <RightOutlined className="controlButtonIcon" />
              )
            }
          />
        </div>
      );
    else return benchAgingLabelsTitle[label];
  };

  const renderCellData = (data, includeJSX, testid) => {
    return includeJSX ? (
      <div className="renderParent" data-testid={testid}>
        <div className="renderData">{data}</div>
        <div className="renderIcon">{<DownloadOutlined />}</div>
      </div>
    ) : (
      data
    );
  };

  /**
   * Fetch column title for child column
   * @param {string} duration Duration of data object
   * @param {boolean} isExpanded Whether column is expanded or not
   * @param {boolean} includeJSX To include or exclude button in columns
   * @returns Title of child column
   */
  const getChildColumnTitle = (duration, isExpanded, includeJSX) => {
    let title = benchAgingDaysTitle[duration];
    if (includeJSX) return title === "Total" && isExpanded ? "" : title;
    else return title;
  };

  /**
   *  Generates table columns and return the column array.
   * @param {array} data Sample data to generate columns.
   * @param {includeJSX} includeJSX To include or exclude button in columns
   * @return {array} Return column array for the table component.
   */
  function getColumns(data, includeJSX) {
    const columns = [];
    //Adding the Capability or Band column
    let newColumn = {
      title: getTitle(benchType),
      width: 180,
      dataIndex: "name",
      render: (record) => record,
      fixed: isFixed(),
    };
    columns.push(newColumn);

    //Days are grouped along the title(Allocation Pending, Nominated, Bench)
    benchAgingLabels.forEach((benchAgingLabel, id) => {
      let new_col = {
        title: getColumnTitle(benchAgingLabel, includeJSX),
        dataIndex: "bench",
        key: benchAgingLabel,
        align: "center",
        width: 180,
        className: "benchStatus",
        //Days are generated under the status(Allocation Pending, Nominated, Bench)
        children: data?.bench?.map((obj, index) => {
          let col = {
            title: getChildColumnTitle(
              obj.duration,
              hiddenTitle[benchAgingLabel],
              includeJSX
            ),
            status: benchAgingDownloadTitle[benchAgingLabel],
            duration: obj.duration,
            startDate: obj.startDate,
            endDate: obj.endDate,
            dataIndex: "bench",
            key: `${benchAgingLabel}${benchAgingDownloadTitle[benchAgingLabel]}`,
            render: (record) =>
              renderCellData(
                record[index][benchAgingLabel],
                includeJSX,
                `${benchAgingLabel}${obj.duration}`
              ),
            hidden:
              benchAgingDaysTitle[obj.duration] === "Total"
                ? false
                : hiddenTitle[benchAgingLabel],
            align: "center",
            width: 180,
            className:
              benchAgingDaysTitle[obj.duration] === "Total"
                ? "benchStatus"
                : "daysRange",
          };
          return col;
        }),
      };
      columns.push(new_col);
      columns.push({
        title: "",
        width: 10,
      });
    });
    //Generates the Grand total column in table.
    let TotalColumn = {
      title: "Grand Total",
      dataIndex: "bench",
      key: "grandTotal",
      align: "center",
      children: [
        {
          title: "",
          dataIndex: "bench",
          status: "TOTAL_BENCH",
          key: "grandTotalChild",
          align: "center",
          render: (record) => {
            let sum = [];
            sum = record.filter((obj) => obj.duration === "ALL_DURATION")[0];
            let total =
              sum.allocationPendingEmployees +
              sum.nominatedEmployees +
              sum.availableEmployees;
            return renderCellData(total, includeJSX, `rowTotal`);
          },
          width: 150,
          className: "grandTotalColumn",
        },
      ],
      width: 150,
      className: "grandTotalColumn",
    };
    columns.push(TotalColumn);

    //Adding On-click property to every cell.
    columns?.map(
      (obj, index) =>
        obj.children &&
        index > 0 &&
        obj?.children?.map(
          (cObj) =>
            (cObj["onCell"] = (record) => {
              return {
                onClick: (e) =>
                  functiondownload(
                    record.isChild,
                    record.name,
                    record.capability,
                    cObj.startDate,
                    cObj.duration,
                    cObj.endDate,
                    cObj.status
                  ),
              };
            })
        )
    );
    return columns;
  }

  //Query to fetch bench_aging by capability data
  const [
    benchAgingByCapability,
    {
      data: capabilityData,
      error: capabilityError,
      loading: capabilityLoading,
    },
  ] = useLazyQuery(GET_BENCH_AGING_BY_CAPABILITY, {
    context: { clientName: "opsMetrics" },
    fetchPolicy: "network-only",
  });

  //Query to fetch bench_aging by band data
  const [
    benchAgingByBand,
    { data: bandData, error: bandError, loading: bandLoading },
  ] = useLazyQuery(GET_BENCH_AGING_BY_BAND, {
    context: { clientName: "opsMetrics" },
    fetchPolicy: "network-only",
  });

  // Query to fetch Bench_aging_band_wise_split data
  const { loading } = useQuery(GET_BENCH_AGING_BAND_WISE_SPLIT, {
    context: { clientName: "opsMetrics" },
    fetchPolicy: "network-only",
    notifyOnNetworkStatusChange: true,
    onCompleted: (data) => {
      let result =
        data?.getBenchAgingBandWiseSplit?.data?.filter(
          (obj) => obj.duration !== "CURRENT_WEEK"
        ) || [];
      setBandSplitData(result);
    },
  });

  // On call Query to fetch the benchAging_Capability_and_Band_Details data
  const [benchAgingCapAndBandDetails, { data: CapDetailData }] = useLazyQuery(
    GET_BENCH_AGING_DETAILS_BY_CAP_OR_BAND,
    {
      context: { clientName: "opsMetrics" },
      fetchPolicy: "network-only",
      onCompleted: () => {
        downloadCellData(
          CapDetailData?.getBenchAgingDetailsByCapabilityAndBand,
          fileName
        );
      },
    }
  );

  // On call Query to fetch the benchAging_GrandTotal_Band_Details data
  const [benchAgingTotalByBand, { data: TotalBandData }] = useLazyQuery(
    GET_BENCH_AGING_DETAILS_BY_TOTAL_BAND,
    {
      context: { clientName: "opsMetrics" },
      fetchPolicy: "network-only",
      onCompleted: () => {
        downloadCellData(
          TotalBandData?.getTotalBenchAgingDetailsByBand,
          fileName
        );
      },
    }
  );

  //On call Query to fetch the benchAging_GrandTotal_Capability_Details data
  const [benchAgingTotalByCap, { data: TotalCapData }] = useLazyQuery(
    GET_BENCH_AGING_DETAILS_BY_TOTAL_CAPABILITY,
    {
      context: { clientName: "opsMetrics" },
      fetchPolicy: "network-only",
      onCompleted: () => {
        downloadCellData(
          TotalCapData?.getTotalBenchAgingDetailsByCapability,
          fileName
        );
      },
    }
  );

  //On call Query to fetch the benchAging_Band_Details data
  const [benchAgingBandDetails, { data: BandDetailData }] = useLazyQuery(
    GET_BENCH_AGING_DETAILS_BY_BAND,
    {
      context: { clientName: "opsMetrics" },
      fetchPolicy: "network-only",
      onCompleted: () => {
        downloadCellData(BandDetailData?.getBenchAgingDetailsByBand, fileName);
      },
    }
  );

  /** Groups the data based on the their bandName.
   *
   * @param {array} data returned by the query.
   * @param {string} type Used to group the data based on the type.
   * @returns {array} returns grouped data based on bandName.
   */
  const groupChildren = (data, type) => {
    if (data) {
      const formattedData = data?.reduce(function (res, obj) {
        res[obj[type]] = res[obj[type]] || [];
        res[obj[type]].push(obj);
        return res;
      }, Object.create(null));

      return formattedData;
    }
  };

  // Grouping of data based on the key bandName
  const childData = groupChildren(bandSplitData, "bandName");

  const getSupplyArray = (supply_arr) => {
    return supply_arr.map((obj) => {
      let temp = obj;
      Object.keys(childData).forEach((childKey) => {
        let bandWiseData = sortByDuration(
          childData[childKey].filter(
            (bandObj) => bandObj.capabilityName === obj.name
          )
        );
        let childObj = {
          key: childKey + obj.name,
          capability: obj.name,
          name: childKey,
          isChild: true,
          bench: bandWiseData,
        };
        if (bandWiseData?.length > 0) {
          if (temp.children) temp["children"].push(childObj);
          else temp["children"] = [childObj];
        }
      });
      return temp;
    });
  };

  const getCapOrBandObj = (dataSource) => {
    let cap_or_band_obj = {};

    //Grouping the data which has the same capability or band name.
    dataSource?.forEach((data) => {
      if (data.duration !== "CURRENT_WEEK") {
        let temp_obj = {
          duration: data.duration,
          allocationPendingEmployees: data.allocationPendingEmployees,
          availableEmployees: data.availableEmployees,
          capabilityName: data.capabilityName,
          bandName: data.bandName,
          nominatedEmployees: data.nominatedEmployees,
          startDate: splitDate(data.startDate),
          endDate: splitDate(data.endDate),
        };

        if (!cap_or_band_obj[data[`${type}Name`]]) {
          cap_or_band_obj[data[`${type}Name`]] = [];
        }
        cap_or_band_obj[data[`${type}Name`]].push(temp_obj);
      }
    });
    return cap_or_band_obj;
  };

  useMemo(() => {
    let dataSource =
      benchType === CAPABILITY
        ? capabilityData?.getBenchAgingByCapability?.data
        : bandData?.getBenchAgingByBand?.data || [];
    if (dataSource?.length > 0) {
      let supply_arr = [];
      let cap_or_band_obj = getCapOrBandObj(dataSource);

      Object.keys(cap_or_band_obj, type).forEach((key) => {
        supply_arr.push({
          name: key,
          bench: sortByDuration(cap_or_band_obj[key]),
          key: key,
        });
      });

      // Append band wise data as a children of each capability
      if (type === "capability") {
        supply_arr = getSupplyArray(supply_arr);
      }

      let exportData = [];
      supply_arr.forEach((obj) => {
        exportData.push(obj);
        if (obj.children) {
          exportData.push(...obj.children);
        }
      });

      setColArray(getColumns(supply_arr[0], true));
      setExportColArray(getColumns(supply_arr[0], false));
      setDataSource(supply_arr?.filter((obj) => obj.name !== "Total"));
      setExportDataSource(exportData?.filter((obj) => obj.name !== "Total"));
      setTableTotal(supply_arr?.filter((obj) => obj.name === "Total")[0]);
    }
  }, [bandData, capabilityData, benchType, flag, bandSplitData]);

  // conditionally calling queries
  useEffect(() => {
    if (benchType === CAPABILITY) {
      benchAgingByCapability();
    } else {
      benchAgingByBand();
    }
  }, [benchType]);

  //Expanding the Labels Title column.
  const expandColumns = (data) => {
    const coloumnData = data?.map((item, index) => {
      if (item.children) {
        let copy = item;
        copy.children = item?.children?.filter((obj) => !obj.hidden);
        return copy;
      }
      return item;
    });
    return coloumnData;
  };

  //On-click query for the total column.
  const onTotalDuration = (obj, i) => {
    if (type === "capability") {
      setFileName(
        obj.capabilityName +
          "-" +
          benchAgingDownloadTitle[i] +
          "-" +
          obj.duration
      );
      benchAgingTotalByCap({
        variables: {
          capabilityName: obj.capabilityName,
          benchType: benchAgingDownloadTitle[i],
        },
      });
    } else {
      setFileName(
        obj.bandName + "-" + benchAgingDownloadTitle[i] + "-" + obj.duration
      );
      benchAgingTotalByBand({
        variables: {
          bandName: obj.bandName,
          benchType: benchAgingDownloadTitle[i],
        },
      });
    }
  };

  //On-click query for the after expanding the column.
  const onTotalDurationExpand = (obj, i) => {
    if (type === "capability" && obj.duration !== "ALL_DURATION") {
      setFileName(
        obj.capabilityName +
          "-" +
          benchAgingDownloadTitle[i] +
          "-" +
          obj.duration
      );
      benchAgingCapAndBandDetails({
        variables: {
          capabilityName: obj.capabilityName,
          bandName: "",
          startDate: obj.startDate,
          endDate: obj.endDate,
          benchType: benchAgingDownloadTitle[i],
        },
      });
    } else if (type === "capability" && obj.duration === "ALL_DURATION") {
      setFileName(
        obj.capabilityName +
          "-" +
          benchAgingDownloadTitle[i] +
          "-" +
          obj.duration
      );
      benchAgingTotalByCap({
        variables: {
          capabilityName: obj.capabilityName,
          benchType: benchAgingDownloadTitle[i],
        },
      });
    } else if (type === "band" && obj.duration === "ALL_DURATION") {
      setFileName(
        obj.bandName + "-" + benchAgingDownloadTitle[i] + "-" + obj.duration
      );
      benchAgingTotalByBand({
        variables: {
          bandName: obj.bandName,
          benchType: benchAgingDownloadTitle[i],
        },
      });
    } else {
      setFileName(
        obj.bandName + "-" + benchAgingDownloadTitle[i] + "-" + obj.duration
      );
      benchAgingBandDetails({
        variables: {
          bandName: obj.bandName,
          startDate: obj.startDate,
          endDate: obj.endDate,
          benchType: benchAgingDownloadTitle[i],
        },
      });
    }
  };

  //On-click query for the grand-total column.
  const onGrandTotal = () => {
    if (type === "capability") {
      setFileName("Total-TOTAL_BENCH");
      benchAgingTotalByCap({
        variables: { capabilityName: "Total", benchType: "TOTAL_BENCH" },
      });
    } else {
      setFileName("Total-TOTAL_BENCH");
      benchAgingTotalByBand({
        variables: { bandName: "Total", benchType: "TOTAL_BENCH" },
      });
    }
  };

  const GrandTotalSum = (duration, sum, num) => {
    if (duration === "ALL_DURATION") {
      sum += num;
    }
    return sum;
  };

  const fetchHeight = () => {
    return window.innerHeight - pageHeaderHeight - 246 || 0;
  };

  useEffect(() => {
    if (capabilityError || bandError) {
      setErrorMessage(Error);
    }
  }, [capabilityError, bandError]);

  return (
    <div className="benchAgingWrapper">
      <div className="benchHeader" id="benchHeader">
        <div className="benchTitle">
          {FREE_POOL_AGEING_TITLE}
          <UserManual reportName={FREE_POOL_AGEING_TITLE} />
        </div>
        <div className="benchRadioButtons">
          <Radio.Group
            value={benchType}
            onChange={(e) => {
              setErrorMessage("");
              onToggle(e);
            }}
          >
            <Radio value={CAPABILITY} className="toggleOptions">
              {BY_CAPABILITY}
            </Radio>
            <Radio value={BAND} className="toggleOptions">
              {BY_BAND}
            </Radio>
          </Radio.Group>
        </div>
        <div className="benchExport">
          <ExportToExcel
            sheetName={"Free Pool Ageing"}
            columnsArray={exportColArray}
            dataSource={exportDataSource}
            total={tableTotal}
          />
        </div>
      </div>
      <div className="benchTableWrapper">
        {errorMessage ? (
          <Result {...errorMessage} />
        ) : (
          <Table
            locale={{
              cancelSort: "Sorted - Descending",
            }}
            rowKey="key"
            loading={capabilityLoading || bandLoading || loading}
            rowClassName="benchAgingRow"
            size="small"
            pagination={false}
            scroll={{
              x: "100%",
              y: fetchHeight(),
            }}
            columns={expandColumns(colArray)}
            dataSource={dataSource}
            summary={() => {
              let sum = 0;
              return (
                /**
                 * Adds summary row containing total
                 * and maps it based on the order of data
                 */
                <Table.Summary fixed bordered={true}>
                  <Table.Summary.Row className="totalSum">
                    <Table.Summary.Cell index={0}>Total </Table.Summary.Cell>
                    {benchAgingLabels.map((i, idx) => {
                      return (
                        <>
                          {idx > 0 && <Table.Summary.Cell></Table.Summary.Cell>}
                          {tableTotal?.bench?.map((obj, index) => {
                            if (hiddenTitle[i] === false) {
                              sum = GrandTotalSum(obj.duration, sum, obj[i]);
                              return (
                                <Table.Summary.Cell align="center">
                                  <div
                                    onClick={() =>
                                      onTotalDurationExpand(obj, i)
                                    }
                                  >
                                    {renderCellData(
                                      obj[i],
                                      true,
                                      `totalExpand${i}${obj.duration}`
                                    )}
                                  </div>
                                </Table.Summary.Cell>
                              );
                            } else {
                              if (index === 4) {
                                sum += obj[i];
                                return (
                                  <Table.Summary.Cell align="center">
                                    <div
                                      onClick={() => onTotalDuration(obj, i)}
                                    >
                                      {renderCellData(
                                        obj[i],
                                        true,
                                        `total${i}${obj.duration}`
                                      )}
                                    </div>
                                  </Table.Summary.Cell>
                                );
                              }
                            }
                          })}
                        </>
                      );
                    })}
                    <Table.Summary.Cell></Table.Summary.Cell>
                    <Table.Summary.Cell align="center">
                      <div onClick={() => onGrandTotal()}>
                        {renderCellData(sum, true, `grandTotal`)}
                      </div>
                    </Table.Summary.Cell>
                  </Table.Summary.Row>
                </Table.Summary>
              );
            }}
          />
        )}
      </div>
    </div>
  );
}
