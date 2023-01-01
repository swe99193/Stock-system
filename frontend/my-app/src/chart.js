import React, { useEffect } from "react";
import * as d3 from "d3";
import './chart.css';

const title = {
    'ratio1': 'Current Ratio',
    'ratio2': 'Long Term Debt / Captial',
    'ratio3': 'Debt / Equity',
    'ratio4': 'Gross Margin',
    'ratio5': 'Operating Margin',
    'ratio6': 'Pre-Tax Profit Margin',
    'ratio7': 'Net Profit Margin',
    'ratio8': 'Asset Turnover',
    'ratio9': 'Inventory Turnover',
    'ratio10': 'Receivable Turnover',
    'ratio11': 'Days Sales In Receivables ',
    'ratio12': 'ROE',
    'ratio13': 'ROTE',
    'ratio14': 'ROA ',
    'ratio15': 'ROI',
    'ratio16': 'Book Value Per Share',
    'ratio17': 'Operating Cash Flow Per Share',
    'ratio18': 'Free Case Flow Per Share',
    'Stock_Return': 'Stock Return'
};

const quarter_to_month = {
    1: '3',
    2: '6',
    3: '9',
    4: '12'
};


/**
 * render the previous 5 quarters before a given timestamp (Year, Quarter)
 * 
 * source code: https://d3-graph-gallery.com/graph/line_basic.html
 */
function Line_chart(props) {
    let data = props.data; // json
    let numbers = data[props.idx]; // array of numbers
    // console.log(numbers);
    var N = numbers.length;

    const createGraph = async() => {
            
       if(data['data_ready']) {
          let countNull = 0
          for (let i = 0; i < numbers.length; i++) {
            if (numbers[i] === null)
              countNull += 1;
          }

          if (countNull<5){
            let parseTime = d3.timeParse("%Y-%m");

            var dataWrapper = new Array();
            
            let curYear = data['Year'], curQuarter = data['Quarter'];

            let q_tmp = curQuarter+1;
            let y_tmp = curYear;
            if (q_tmp>4) {  q_tmp%=4; y_tmp += 1;}
            let endTime = parseTime(y_tmp.toString() + "-" + quarter_to_month[q_tmp]);
            for (let i = N-1; i >=0; i--) {
                let time = curYear.toString() + "-" + quarter_to_month[curQuarter];
                
                let obj = new Object();
                if (numbers[i] !== null) {
                  obj.value = numbers[i];
                  obj.year = curYear;
                  obj.quarter = curQuarter;
                  obj.date = parseTime(time);

                  dataWrapper.unshift(obj) 
                }
                if (curQuarter == 1) {
                    curQuarter += 4;
                    curYear -= 1;
                }
                curQuarter -= 1;
            }
            let startTime = parseTime(curYear.toString() + "-" + quarter_to_month[curQuarter])

            console.log(dataWrapper);

            // console.log(Object.keys(numbers).length);

            // set the dimensions and margins of the graph
            var margin = { top: 20, right: 20, bottom: 50, left: 70 },
                width = 600 - margin.left - margin.right,
                height = 400 - margin.top - margin.bottom;

            // append the svg object to the target label 
            var svg = d3.select("#" + props.idx).append("svg")
                // .attr("width", width + margin.left + margin.right)
                // .attr("height", height + margin.top + margin.bottom)
                .attr("preserveAspectRatio", "xMinYMin meet")
                .attr("viewBox", "0 0 600 500")
                .append("g")
                .attr("transform", `translate(${margin.left}, ${margin.top})`);

            // create X axis and Y axis
            var x = d3.scaleTime().range([0, width]);
            var y = d3.scaleLinear().range([height, 0]);

            let y_min = d3.min(dataWrapper, (d) => { return d.value; });
            let y_max = d3.max(dataWrapper, (d) => { return d.value; });
            x.domain([startTime, endTime]);
            y.domain([y_min - (y_max-y_min)/10 ,y_max + (y_max-y_min)/10 ]);

            // x-axis
            svg.append("g")
                .attr("transform", `translate(0, ${height})`)
                .call(d3.axisBottom(x).ticks(5).tickFormat(d3.timeFormat('%Y/%m')));  // adjust x-axis ticks

            // y-axis
            svg.append("g")
                .call(d3.axisLeft(y));


            // add the line
            // var valueLine = d3.line()
            //     .x((d) => { return x(d.date); })
            //     .y((d) => { return y(d.value); });

            // svg.append("path")
            //     .data([dataWrapper])
            //     .attr("class", "line")
            //     .attr("fill", "none")
            //     .attr("stroke", "orange")
            //     .attr("stroke-width", 2.0)
            //     .attr("d", valueLine);

            // render static points
            for (let i = 0; i < dataWrapper.length; i++) { 
              var point = svg
              .append('g')
              .append('circle')
                .style("fill", "orange")
                .attr("stroke", "orange")
                .attr('r', 5)
                .style("opacity", 1)
                .attr("cx", x(dataWrapper[i].date))
                .attr("cy", y(dataWrapper[i].value));
            }

            // Create interactive points that travels along the curve of chart
            var focus = svg
            .append('g')
            .append('circle')
              .style("fill", "orange")
              .attr("stroke", "white")
              .attr('r', 10.0)
              .style("opacity", 0);


          // Create the text that travels along the curve of chart
          var focusText = svg
            .append('g')
            .append('text')
              .style("fill", 'white')
              .style("opacity", 0)
              .attr("text-anchor", "left")
              .attr("alignment-baseline", "middle");
          var focusText2 = svg
            .append('g')
            .append('text')
              .style("fill", 'white')
              .style("opacity", 0)
              .attr("text-anchor", "left")
              .attr("alignment-baseline", "middle");


            // This allows to find the closest X index of the mouse:
            var bisect = d3.bisector((d) => { return d.date; }).left;


              // Create a rect on top of the svg area: this rectangle recovers mouse position
              svg
              .append('rect')
              .style("fill", "none")
              .style("pointer-events", "all")
              .attr('width', width)
              .attr('height', height)
              .on('mouseover', mouseover)
              .on('mousemove', mousemove)
              .on('mouseout', mouseout);


            // What happens when the mouse move -> show the annotations at the right positions.
            function mouseover() {
              focus.style("opacity", 1)
              focusText.style("opacity",1)
              focusText2.style("opacity",1)
            }

            function mousemove() {
              // recover coordinate we need
              let x0 = x.invert(d3.mouse(this)[0]);
              let i = bisect(dataWrapper, x0, 1) - 1;
              let selectedData = dataWrapper[i];

              focus
                .attr("cx", x(selectedData.date))
                .attr("cy", y(selectedData.value));
              focusText
                .html("Value:" + selectedData.value)
                .attr("x", x(selectedData.date)+15)
                .attr("y", y(selectedData.value)-10);
              focusText2
                .html(selectedData.year + '/Q' + selectedData.quarter)
                .attr("x", x(selectedData.date)+15)
                .attr("y", y(selectedData.value)+10);
              }
            function mouseout() {
              focus.style("opacity", 0);
              focusText.style("opacity", 0);
              focusText2.style("opacity", 0);
          }
        }
      }

    }

    useEffect(() => {
        createGraph();
    }, [props]);

    return ( 
    <div id = { props.idx } > { title[props.idx] } </div>
    );
}

export default Line_chart;