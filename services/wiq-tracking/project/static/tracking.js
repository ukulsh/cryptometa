$(document).ready(function() {
  const urlParams = new URLSearchParams(window.location.search);
  let invalidTracking = urlParams.get('invalid');
  if(invalidTracking) {
    let trackingError = document.querySelector('.tracking-error');
    trackingError.innerHTML = invalidTracking;
    trackingError.classList.remove('hide');
  }

  let orderIdRadio = document.querySelector('#orderId');
  let awbRadio = document.querySelector('#awbNumber');

  var selectedRadio = orderIdRadio.checked ? 'orderId' : 'awbNumber';

  let orderIdSearch = document.querySelector('#orderid-search-container');
  let awbSearch = document.querySelector('#awb-search-container');

  let trackBtn = document.querySelector('#trackSubmitBtn');

  let orderInput = document.querySelector('#order_id_input');
  let mobileInput = document.querySelector('#mobile_number_input');
  let awbInput = document.querySelector('#awb_number_input');

  let inputs = {
    orderId: orderInput.value || '',
    mobile: mobileInput.value || '',
    awb: awbInput.value || ''
  }

  orderIdRadio.addEventListener('change',onRadioChange,false);
  awbRadio.addEventListener('change',onRadioChange,false);

  onRadioChange({target: {value: selectedRadio}});

  function onRadioChange(e) {
    let value = e.target.value;
    if(value == 'orderId') {
      orderIdSearch.classList.remove('hide');
      awbSearch.classList.add('hide');
      selectedRadio = 'orderId';
      orderIdRadio.setAttribute('checked','');
      awbRadio.removeAttribute('checked');
    } else if(value == 'awbNumber') {
      orderIdSearch.classList.add('hide');
      awbSearch.classList.remove('hide');
      selectedRadio = 'awbNumber';
      awbRadio.setAttribute('checked','');
      orderIdRadio.removeAttribute('checked');
    }
    checkTrackBtn();
  }

  orderInput.addEventListener('keyup',inputChange,false);
  mobileInput.addEventListener('keyup',inputChange,false);
  awbInput.addEventListener('keyup',inputChange,false);

  function inputChange(e) {
    let name = e.target.name;
    let value = e.target.value;
    inputs[name] = value;
    checkTrackBtn();
  }

  function checkTrackBtn() {
    let formComplete = false;
    if(selectedRadio == 'orderId') {
      if(inputs['orderId'] && inputs['mobile']) {
        formComplete = true
      }
    } else {
      if(inputs['awb']) {
        formComplete = true
      }
    }
    trackBtn.classList[formComplete ? 'remove' : 'add']('disabled');
  }

  trackBtn.addEventListener('click',onTrackClick,false);

  function onTrackClick() {
    let url = `${window.origin}/tracking`;
    if(selectedRadio == 'orderId') {
      url += `?orderId=${inputs['orderId']}&mobile=${inputs['mobile']}`;
    } else {
      url += `/${inputs['awb']}`;
    }
    window.location.href = url;
  }
})