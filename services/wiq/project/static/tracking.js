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

  var selectedRadio = orderIdRadio && orderIdRadio.checked ? 'orderId' : 'awbNumber';

  let orderIdSearch = document.querySelectorAll('.orderid-search-container');
  let awbSearch = document.querySelectorAll('.awb-search-container');

  let trackBtn = document.querySelector('#trackSubmitBtn');

  let orderInput = document.querySelector('#order_id_input');
  let mobileInput = document.querySelector('#mobile_number_input');
  let awbInput = document.querySelector('#awb_number_input');

  let inputs = {
    orderId: orderInput && orderInput.value || '',
    mobile: mobileInput && mobileInput.value || '',
    awb: awbInput && awbInput.value || ''
  }

  orderIdRadio && orderIdRadio.addEventListener('change',onRadioChange,false);
  awbRadio && awbRadio.addEventListener('change',onRadioChange,false);

  onRadioChange({target: {value: selectedRadio}});

  function onRadioChange(e) {
    let value = e.target.value;
    if(value == 'orderId') {
      if(orderIdSearch && orderIdSearch.length > 0) {
        for(let i = 0; i < orderIdSearch.length; i++) {
          orderIdSearch[i].classList.remove('hide');
        }
      }
      if(awbSearch && awbSearch.length > 0) {
        for(let i = 0; i < awbSearch.length; i++) {
          awbSearch[i].classList.add('hide');
        }
      }
      selectedRadio = 'orderId';
      orderIdRadio && orderIdRadio.setAttribute('checked','');
      awbRadio && awbRadio.removeAttribute('checked');
    } else if(value == 'awbNumber') {
      if(orderIdSearch && orderIdSearch.length > 0) {
        for(let i = 0; i < orderIdSearch.length; i++) {
          orderIdSearch[i].classList.add('hide');
        }
      }
      if(awbSearch && awbSearch.length > 0) {
        for(let i = 0; i < awbSearch.length; i++) {
          awbSearch[i].classList.remove('hide');
        }
      }
      selectedRadio = 'awbNumber';
      awbRadio && awbRadio.setAttribute('checked','');
      orderIdRadio && orderIdRadio.removeAttribute('checked');
    }
    checkTrackBtn();
  }

  orderInput && orderInput.addEventListener('keyup',inputChange,false);
  mobileInput && mobileInput.addEventListener('keyup',inputChange,false);
  awbInput && awbInput.addEventListener('keyup',inputChange,false);

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
    trackBtn && trackBtn.classList[formComplete ? 'remove' : 'add']('disabled');
  }

  trackBtn && trackBtn.addEventListener('click',onTrackClick,false);

  function onTrackClick() {
    let url = `${window.origin}/tracking`;
    if(selectedRadio == 'orderId') {
      url += `?orderId=${inputs['orderId']}&mobile=${inputs['mobile']}`;
    } else {
      url += `/${inputs['awb']}`;
    }
    window.location.href = url;
  }
  if(document.querySelector('#tracking-banners-carousel')) {
    $('#tracking-banners-carousel').slick({
      slidesToShow: 3,
      arrows: false,
      autoplay: true,
      dots: true,
      appendDots: $('.banner-carousel-dots-container'),
      dotsClass: 'banner-carousel-dots',
      infinite: true,
      autoplay: true,
      autoplaySpeed: 4000,
      responsive: [
        {
          breakpoint: 1024,
          settings: {
            slidesToShow: 2,
          }
        },
        {
          breakpoint: 577,
          settings: {
            slidesToShow: 1,
          }
        }
      ]
    });
  }
})