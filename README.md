## 프로젝트 개요
### 배경 및 소개
최근 글로벌 경제 환경은 급변하고 있으며, 금리 인상, 인플레이션, 원자재 가격 변동과 같은 경제 이슈들 간에 미치는 영향에 대한 관심이 높아지고 있습니다. 
</br>
이에, **환율과 주요 경제 지표 간의 상관관계를 분석**하고, 이를 쉽게 이해할 수 있도록 시각화하였습니다. 
</br>
또한, **slack bot**을 통해 환율과 주요 경제지표 및 등락률에 대한 데이터 접근성을 높이고자 합니다.
</br>
</br>
## 📍 프로젝트 시각화 결과
### 오늘 환율 및 전일 대비 상승률 & 환율 변동 차트
<table>
  <tr>
    <td align="center">
      <img src="https://github.com/yygs321/currenomics/blob/main/images/01_today_exchange_rate.png"">
    </td>
  </tr>
  <tr>
    <td align="center">
      <span>오늘 환율 및 전일 대비 상승률</span>
    </td>
  </tr>
</table>
<table>
  <tr>
    <td align="center">
      <img src="https://github.com/yygs321/currenomics/blob/main/images/02_yearly_rates.png">
    </td>
  </tr>
  <tr>
    <td align="center">
      <span>환율 변동 차트</span>
    </td>
  </tr>
</table>

### 최근 경제이슈 테이블
<table>
  <tr>
    <td align="center">
      <img src="https://github.com/yygs321/currenomics/blob/main/images/05_2_issue_table.png">
    </td>
  </tr>
  <tr>
    <td align="center">
      <span>최근 경제 이슈 </span>
    </td>
  </tr>
</table>

### 환율 변화와 경제 지표 상관 관계 분석
<table>
  <tr>
    <td align="center">
      <img src="https://github.com/yygs321/currenomics/blob/main/images/11_rates_air.png">
    </td>
    <td align="center">
      <img src="https://github.com/yygs321/currenomics/blob/main/images/09_fed_krw.png">
    </td>
  </tr>
  <tr>
    <td align="center">
      <span>환율 변화와 항공 여객 수송 지수</span>
    </td>
    <td align="center">
      <span>환율 변화와 미국 기준 금리</span>
    </td>
  </tr>
</table>
<ul>
  <li>코로나 19 바이러스로 인해 항공 여객 수송지수의 급격한 하락</li>
  <li>반면, 환율에는 큰 영향은 없는 것으로 보임</li>
  ⇒ 코로나19로 인한 경제적 충격이 전 세계적으로 비슷한 시기에 일어났고, 각국의 경제 정책과 금융 시장 반응도 유사했기에 환율 변동성에 크게 차이를 보이지 않는 것으로 보인다.
</ul>
<ul>
  <li>금리 인상에 따른 원화와 엔화, 유로의 변화</li>
  <ul>
    <li>금리 인상에 따른 원화와 엔화의 급등</li>
    <li>반면, 유로는 금리 인상에 따라 오히려 환율이 내려가는 양상</li>
  </ul>
  ⇒ 동아시아 환율의 지표가 전부 올라간 것으로 보아 서방 국가들의 통화 가치 하락이 이에 영향을 준 것으로 예상된다.
</ul>



### 환율 및 브렌트유 가격과 한국의 무역수지 상관 관계

<table>
  <tr>
    <td align="center">
      <img src="https://github.com/yygs321/currenomics/blob/main/images/13_brent_trade.png"/>
    </td>
    <td align="center">
      <img src="https://github.com/yygs321/currenomics/blob/main/images/12_krw_trade.png" />
    </td>
  </tr>
  <tr>
    <td align="center">
      <span>브렌트유 가격과 한국 무역수지</span>
    </td>
    <td align="center">
      <span>원-달러 환율과 한국의 무역수지</span>
    </td>
  </tr>
</table>
<ul>
  <li>브렌트유 가격과 한국 무역수지 값이 <strong>완전히 반대</strong>되는 결과가 나타남</li>
  ⇒브렌트유 가격 상승으로 한국이 원유를 수입하는 비용이 커지며 무역수지가 하락한다.
</ul>

<ul>
  <li>환율 상승 시(= 원화가치 하락) 한국 무역수지가 하락/ 환율 하락 시 한국 무역수지가 상승</li>
  ⇒환율 변화에 따라 수입 원자재 가격 상승/하락하며 무역수지에도 영향을 미치게 된다.
</ul>


### 미국 기준 금리와 경제 지표 분석
<table>
  <tr>
    <td>
      <img src="https://github.com/yygs321/currenomics/blob/main/images/06_fed_employment.png.png" />
    </td>
      <td>
      <img src="https://github.com/yygs321/currenomics/blob/main/images/07_fed_sp500.png.png" />
    </td>
  </tr>
  <tr>
    <td align="center">
      <span>미국 기준 금리와 한국 고용률 상관관계</span>
    </td>
    <td align="center">
      <span>미국 기준 금리와 S&P 500지수 상관관계</span>
    </td>
  </tr>
</table>
<ul>
  <li>2020년 코로나 팬데믹 영향으로 인해 미국 금리가 떨어지면서 투자가 활발해짐에 따라 S&P 500 지수가 증가.</li>
</ul>
<ul>
  <li>코로나 팬데믹 이후 미국 기준금리는 내려갔지만, 한국 고용률(실업률) 간의 관계에는 특별한 상관관계는 보이지 않음</li>
  ⇒ 고용률과 실업률은 금리 변화 외에도 노동 시장의 유연성, 산업 구조 변화 등 다양한 다른 요인들에 영향을 받을 수 있다.
</ul>


### 원-달러 환율과 경제 지표 분석
<table>
  <tr>
    <td align="center">
      <img src="https://github.com/yygs321/currenomics/blob/main/images/03_krw_brent.png">
    </td>
    <td align="center">
      <img src="https://github.com/yygs321/currenomics/blob/main/images/08_sp500_krw.png">
    </td>
  </tr>
  <tr>
    <td align="center">
      <span>원-달러 환율과 브렌트유 가격</span>
    </td>
    <td align="center">
      <span>원-달러 환율과 S&P500지수</span>
    </td>
  </tr>
</table>
<ul>
  <li>브렌트유 가격은 2020년대 초 급락하고, 2022년 중기에 급증</li>
  <ul>
    <li>2020년대 초반: COVID-19 팬데믹으로 인해 이동이 제한 되면서 원유 수요가 급감했고, OPEC+ 협정 불발로 인해 석유 생산량이 증가.</li>
    <li>2022년대 중반: 러시아-우크라이나 전쟁과 팬데믹 이후 수요 증가, 원자재 공급망 문제 및 물류비 상승으로 물가 상승 초래.</li>
  </ul>
</ul>

<ul>
  <li>S&P500 지수와 원-달러 환율 간 관계는 반비례 관계</li>
  <ul>
    <li>S&P 500 지수가 상승 = 미국 경제가 강세</li>
  </ul>
  ⇒ 글로벌 경제가 불안정할 경우, 투자자들은 안전 자산인 미국 달러에 자금을 몰리게 되며, 원화 가치 하락을 초래
</ul>

<table>
  <tr>
    <td align="center">
      <img src="https://github.com/yygs321/currenomics/blob/main/images/04_krw_cpi.png" />
    </td>
    <td align="center">
      <img src="https://github.com/yygs321/currenomics/blob/main/images/10_cpi_ppi.png" />
    </td>
  </tr>
  <tr>
    <td align="center">
      <span>원-달러 환율과 한국 소비자 물가지수</span>
    </td>
    <td align="center">
      <span>소비자 물가지수와 생산자 물가지수</span>
    </td>
  </tr>
</table>
<ul>
  <li>환율이 급격히 상승한 2022년부터 한국 소비자 물가지수도 상승하는 추세</li>
  <ul>
    <li>환율 변화에 의한 소비자 물가지수 변화는 즉각적인 영향이 아닌 몇 개월의 시차가 발생할 수 있다.</li>
  </ul>
  ⇒원화 가치가 하락하고 환율이 상승하면 수입 물가가 상승하여 소비자 물가지수가 증가
</ul>

<ul>
  <li>소비자 물가 지수는 꾸준히 상승/ 생산자 물가 지수는 상승 후 하락</li>
  ⇒생산자 물가 지수가 떨어지더라도 이는 소비자 물가에 반영이 되지 않는 것을 알 수 있다.
</ul>
</br>

## 슬랙 봇 활용
<table>
  <tr>
    <td align="center">
      <img src="https://github.com/Currenomics/currenomics/blob/main/images/slack_bot.gif">
    </td>
  </tr>
  <tr>
    <td align="center">
      <span>슬랙 봇으로 주요 국가 환율, S&P500 지수, 브렌트유 가격 확인</span>
    </td>
  </tr>
</table>
</br>
</br>

## ⚙System Architecture

### 시스템 아키텍처

<table>
  <td align="center">
    <img src="https://github.com/yygs321/currenomics/blob/main/images/project_architecture.png">
  </td>
</table>

### 데이터 ETL 과정 상세
<table>
  <td align="center">
    <img src="https://github.com/yygs321/currenomics/blob/main/images/ETL_process.png">
  </td>
</table>

</br>
</br>

## 💾 테이블 명세
<table>
  <tr>
    <td align="center">
        <img src="https://github.com/yygs321/currenomics/blob/main/images/raw_data.png">
    </td>
    <td align="center">
        <img src="https://github.com/yygs321/currenomics/blob/main/images/analytics.png">
    </td>
  </tr>
  <tr>
    <td align="center">
      <b>raw_data</b>
    </td>
    <td align="center">
      <b>analytics_data</b>
    </td>
  </tr>
</table>

</br>
</br>

##  🛠 기술 스택

<h3 align="center">ETL & ELT</h3>
<p align="center">
    <img src="https://img.shields.io/badge/Python-3776AB?&logo=python&logoColor=white">
    <img src="https://img.shields.io/badge/snowflake-29B5E8?&logo=snowflake&logoColor=white">
    <img src="https://img.shields.io/badge/amazons3-569A31?&logo=amazons3&logoColor=white">
    <img src="https://img.shields.io/badge/pandas-150458?&logo=pandas&logoColor=white">
</p>
<h3 align="center">시각화</h3>
<p align="center">
    <img src="https://img.shields.io/badge/preset-00B992?logoColor=white" alt="preset" />
</p>
<h3 align="center">Co-work tool</h3>
<p align="center">
    <img src="https://img.shields.io/badge/docker-2496ED?&logo=docker&logoColor=white">
    <img src="https://img.shields.io/badge/github-181717?&logo=github&logoColor=white">
    <img src="https://img.shields.io/badge/Notion-232F3E?&logo=Notion&logoColor=white">
    <img src="https://img.shields.io/badge/slack-E4637C?&logo=slack&logoColor=white">
</p>

</br>
</br>

## 😊멤버

<table>
  <tr>
    <td align="center">
      <a href="https://github.com/zoobeancurd">
        <img src="https://github.com/zoobeancurd.png" alt="강명주" />
      </a>
    </td>
     <td align="center">
      <a href="https://github.com/Nyhazzy">
        <img src="https://github.com/Nyhazzy.png" alt="김나연" />
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/Mollis-Kim">
        <img src="https://github.com/Mollis-Kim.png" alt="김선재" />
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/yygs321">
        <img src="https://github.com/yygs321.png" alt="박소민" />
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/newskyy135">
        <img src="https://github.com/newskyy135.png" alt="윤성민" />
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/choyeonkyus">
        <img src="https://github.com/choyeonkyu.png" alt="조연규" />
      </a>
    </td>
  </tr>
  <tr>
    <td align="center">
      <a href="https://github.com/zoobeancurd">
        <b>강명주</b>
      </a>
    </td>
     <td align="center">
      <a href="https://github.com/Nyhazzy">
        <b>김나연</b>
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/Mollis-Kimy">
        <b>김선재</b>
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/yygs321">
        <b>박소민</b>
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/newskyy135">
        <b>윤성민</b>
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/choyeonkyu">
        <b>조연규</b>
      </a>
    </td>
  </tr>
  <tr>
    <td align="center">
      <span>ETL&ELT 구축<br>시각화</span>
    </td>
    <td align="center">
      <span>ETL 구축<br>시각화</span>
    </td>
    <td align="center">
      <span>인프라 구축<br>ETL 구축<br>시각화</span>
    </td>
    <td align="center">
      <span>ETL 구축<br>시각화</span>
    </td>
    <td align="center">
      <span>ETL 구축<br>시각화</span>
    </td>
    <td align="center">
      <span>슬랙봇 구현<br>ETL 구축<br>시각화</span>
    </td>
  </tr>
</table>
