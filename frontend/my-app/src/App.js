import logo from './logo.svg';
import './App.css';

import React, { useState, useEffect } from "react";

// chart.js
import Line_chart from './chart';
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import 'bootstrap/dist/css/bootstrap.min.css';

import { useSearchParams, useParams, Form } from "react-router-dom";

import axios from "axios";


const FrontendBaseURL = process.env.REACT_APP_FrontendBaseURL;
const BackendBaseURL = process.env.REACT_APP_BackendBaseURL;

function App(props) {
  // a sample data
  let sample_data = {  
    "data_ready": false,
    "CO_NAME": "",
    // "CO_NAME": "SAMPLE Company",
    "CO_ID": "",
    // "CO_ID": "XXXX.TW",
    "Year": 2019,
    "Quarter": 4,
    "Sector": "Financial Services",
    "ratio1": [-3, 20, 30, 15, 23],
    "ratio2": [5, 2, 3.9, 1, 2.5],
    "ratio3": [0.010, 0.032, 0.021, 0.2, 0.5],
    "ratio4": [0.010, 0.032, 0.021, 0.015, 0.023],
    "ratio5": [0.10, 0.32, 0.21, 0.15, 0.23],
    "ratio6": [0.10, 0.32, 0.21, 0.15, 0.23],
    "ratio7": [0.10, 0.32, 0.21, 0.15, 0.23],
    "ratio8": [0.10, 0.32, 0.21, 0.15, 0.23],
    "ratio9": [0.10, 0.32, 0.21, 0.15, 0.23],
    "ratio10": [0.10, 0.32, 0.21, 0.15, 0.23],
    "ratio11": [0.10, 0.32, 0.21, 0.15, 0.23],
    "ratio12": [0.10, 0.32, 0.21, 0.15, 0.23],
    "ratio13": [0.10, 0.32, 0.21, 0.15, 0.23],
    "ratio14": [0.10, 0.32, 0.21, 0.15, 0.23],
    "ratio15": [0.10, 0.32, 0.21, 0.15, 0.23],
    "ratio16": [0.10, 0.32, 0.21, 0.15, 0.23],
    "ratio17": [0.10, 0.32, 0.21, 0.15, 0.23],
    "ratio18": [0.10, 0.32, 0.21, 0.15, 0.23],
    "Stock_Return": [0.10, 0.32, 0.21, 0.15, 0.23]
  };
  console.log(sample_data);


  const { CO_ID } = useParams();
  const [searchParams] = useSearchParams();
  const [data, setData] = useState(sample_data);
  
  var Year = searchParams.get('Year');
  var Quarter = searchParams.get('Quarter');


  // Fetch data from backend
  const fetchData = async () => {
    const response = await axios.get(
      `${BackendBaseURL}/company_data/${CO_ID}/latest_final_data?Year=${Year}&Quarter=${Quarter}`
      // `http://localhost:8000/company_data/${CO_ID}/latest_final_data?Year=${Year}&Quarter=${Quarter}`
      );

      let newData = response.data

      if (newData && Year >= 2013 && 1<=Quarter && Quarter<=4) {
        newData['data_ready'] = true;
        setData(newData);
      }
      
      console.log(newData);
    };
// `http://localhost:8000/company_data/${CO_ID}/final_data?Year=${Year}&Quarter=${Quarter}`
// `${BackendBaseURL}/company_data/${CO_ID}/final_data?Year=${Year}&Quarter=${Quarter}`

  useEffect(() => {
    fetchData();
  }, []);


  // if (!data['data_exist']){
  //   return (
  //   <Container>
  //     <Row>      
  //       <h1> No data available. </h1>
  //     </Row>
  //   </Container>
  //   );
  // }
  // else{
    function configureFormURL(event){
      // console.log(event);
      event.preventDefault();

      let co_id = document.getElementById("co_id").value;
      let y = document.getElementById("Year").value;
      let q = document.getElementById("Quarter").value;
      let url = `${FrontendBaseURL}/company_data/${co_id}?Year=${y}&Quarter=${q}`;
      // console.log(url);
      document.getElementById('searchForm').setAttribute('action', url);
      document.getElementById("co_id").remove();
      // console.log(document.getElementById('searchForm').getAttribute('action'));
      document.getElementById('searchForm').submit();
    }


    return (
      <Container fluid style={{'width': '90%', 'margin': '30px'}}>
      <Row>
        <form id="searchForm"  method="GET" onSubmit={configureFormURL}>&nbsp;&nbsp;&nbsp;
          <label for="co_id">Company Stock ID&#40;台股&#41;:</label>&nbsp;&nbsp;
          <input type="text" id="co_id" name="co_id" placeholder="2330.TW" size="10" required/>&nbsp;&nbsp;&nbsp;
          <label for="Year">Year:</label>&nbsp;&nbsp;
          <input type="text" id="Year" name="Year" placeholder="2022" size="6" required/>&nbsp;&nbsp;&nbsp;
          <label for="Quarter">Quarter&#40;1~4&#41;:</label>&nbsp;&nbsp;
          <input type="text" id="Quarter" name="Quarter" placeholder="1" size="2" required/>&nbsp;&nbsp;
          <input type="submit" value="Search"/> 
        </form> 
      </Row>
      <br></br>
      <Row>
        <h1> {data['CO_NAME']} <div className='company_id'> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{data['CO_ID']}&nbsp;&nbsp;&nbsp;&nbsp;{data['Year']}&nbsp;Q{data['Quarter']} </div> </h1> 
      </Row>
      <Row>
        <Col><Line_chart idx="ratio1" data={data}/></Col>
        <Col><Line_chart idx="ratio2" data={data}/></Col>

      </Row>
      <Row>
        <Col><Line_chart idx="ratio3" data={data}/></Col>
        <Col><Line_chart idx="ratio4" data={data}/></Col>
      </Row>
      <Row>
        <Col><Line_chart idx="ratio5" data={data}/></Col>
        <Col><Line_chart idx="ratio6" data={data}/></Col>
      </Row>
      <Row>
        <Col><Line_chart idx="ratio7" data={data}/></Col>
        <Col><Line_chart idx="ratio8" data={data}/></Col>
      </Row>
      <Row>
        <Col><Line_chart idx="ratio9" data={data}/></Col>
        <Col><Line_chart idx="ratio10" data={data}/></Col>
      </Row>
      <Row>
        <Col><Line_chart idx="ratio11" data={data}/></Col>
        <Col><Line_chart idx="ratio12" data={data}/></Col>
      </Row>
      <Row>
        <Col><Line_chart idx="ratio13" data={data}/></Col>
        <Col><Line_chart idx="ratio14" data={data}/></Col>
      </Row>
      <Row>
        <Col><Line_chart idx="ratio15" data={data}/></Col>
        <Col><Line_chart idx="ratio16" data={data}/></Col>
      </Row>
      <Row>
        <Col><Line_chart idx="ratio17" data={data}/></Col>
        <Col><Line_chart idx="ratio18" data={data}/></Col>
      </Row>
      <Row>
        <Col><Line_chart idx="Stock_Return" data={data}/></Col>
        <Col></Col>
      </Row>
    </Container>
    );
  // }
}

export default App;
