(function(){
	const css = `
    .toastr-container {
      position: fixed; top:20px; right:20px; display:flex;
      flex-direction:column; gap:10px; z-index:9999; pointer-events:none;
    }
    .toastr {
      min-width:250px; max-width:350px; padding:12px 16px;
      border-radius:6px; color:#fff; font-family:sans-serif;
      box-shadow:0 2px 12px rgba(0,0,0,0.25);
      display:flex; justify-content:space-between; align-items:flex-start;
      opacity:0; transform:translateX(100%); transition:0.3s; pointer-events:auto;
    }
    .toastr.show { opacity:1; transform:translateX(0); }
    .toastr.success{background:#4caf50;}
    .toastr.error{background:#f44336;}
    .toastr.warning{background:#ff9800;}
    .toastr.info{background:#2196f3;}
    .toastr .title{font-weight:bold;margin-bottom:4px;}
    .toastr button{
      background:transparent;border:none;color:#fff;font-weight:bold;
      cursor:pointer;margin-left:10px;font-size:16px;line-height:1;
    }
  `;
	const style = document.createElement('style'); style.textContent = css;
	document.head.appendChild(style);

	function initContainer(){
		let c = document.querySelector('.toastr-container');
		if(!c){
			c = document.createElement('div'); c.className='toastr-container';
			document.body.appendChild(c);
		}
		return c;
	}

	function createToast(type, title, msg, duration=3000){
		const container = initContainer();
		const t = document.createElement('div'); t.className=`toastr ${type}`;
		t.innerHTML = `<div>${title?'<div class="title">'+title+'</div>':''}<div class="message">${msg}</div></div>`;
		const btn = document.createElement('button'); btn.innerHTML='&times;';
		btn.onclick = ()=>hide(t); t.appendChild(btn);
		container.appendChild(t);
		requestAnimationFrame(()=>t.classList.add('show'));
		if(duration>0) setTimeout(()=>hide(t), duration);
		function hide(el){el.classList.remove('show'); setTimeout(()=>el.remove(),300);}
	}

	window.success = (a,b)=>createToast('success', b?a:'', b?b:a);
	window.error   = (a,b)=>createToast('error', b?a:'', b?b:a);
	window.warning = (a,b)=>createToast('warning', b?a:'', b?b:a);
	window.info    = (a,b)=>createToast('info', b?a:'', b?b:a);

	if(document.readyState==='loading') document.addEventListener('DOMContentLoaded', initContainer);
	else initContainer();
})();
