/*  Construido como parte da disciplina: FPPD - PUCRS - Escola Politecnica
    Professor: Fernando Dotti  (https://fldotti.github.io/)
    Modulo representando Algoritmo de Exclusão Mútua Distribuída:
    Semestre 2023/1
	Aspectos a observar:
	   mapeamento de módulo para estrutura
	   inicializacao
	   semantica de concorrência: cada evento é atômico
	   							  módulo trata 1 por vez
	Q U E S T A O
	   Além de obviamente entender a estrutura ...
	   Implementar o núcleo do algoritmo ja descrito, ou seja, o corpo das
	   funcoes reativas a cada entrada possível:
	   			handleUponReqEntry()  // recebe do nivel de cima (app)
				handleUponReqExit()   // recebe do nivel de cima (app)
				handleUponDeliverRespOk(msgOutro)   // recebe do nivel de baixo
				handleUponDeliverReqEntry(msgOutro) // recebe do nivel de baixo
*/

package DIMEX

import (
	PP2PLink "SD/PP2PLink"
	"fmt"
	"strings"
)

// ------------------------------------------------------------------------------------
// ------- principais tipos
// ------------------------------------------------------------------------------------

type State int // enumeracao dos estados possiveis de um processo
const (
	noMX State = iota
	wantMX
	inMX
)

type dmxReq int // enumeracao dos estados possiveis de um processo
const (
	ENTER dmxReq = iota
	EXIT
	//
	SNAPSHOT
)

type dmxResp struct { // mensagem do módulo DIMEX infrmando que pode acessar - pode ser somente um sinal (vazio)
	// mensagem para aplicacao indicando que pode prosseguir
}

// State: bool emSnapshot = false
// 	int idSnap

type DIMEX_Module struct {
	Req       chan dmxReq  // canal para receber pedidos da aplicacao (REQ e EXIT)
	Ind       chan dmxResp // canal para informar aplicacao que pode acessar
	addresses []string     // endereco de todos, na mesma ordem
	id        int          // identificador do processo - é o indice no array de enderecos acima
	st        State        // estado deste processo na exclusao mutua distribuida
	waiting   []bool       // processos aguardando tem flag true
	lcl       int          // relogio logico local
	reqTs     int          // timestamp local da ultima requisicao deste processo
	nbrResps  int
	dbg       bool

	Pp2plink *PP2PLink.PP2PLink // acesso aa comunicacao enviar por PP2PLinq.Req  e receber por PP2PLinq.Ind
}

// ------------------------------------------------------------------------------------
// ------- inicializacao
// ------------------------------------------------------------------------------------

func NewDIMEX(_addresses []string, _id int, _dbg bool) *DIMEX_Module {

	p2p := PP2PLink.NewPP2PLink(_addresses[_id], _dbg)

	dmx := &DIMEX_Module{
		Req: make(chan dmxReq, 1),
		Ind: make(chan dmxResp, 1),

		addresses: _addresses,
		id:        _id,
		st:        noMX,
		waiting:   make([]bool, len(_addresses)),
		lcl:       0,
		reqTs:     0,
		dbg:       _dbg,

		Pp2plink: p2p}

	for i := 0; i < len(dmx.waiting); i++ {
		dmx.waiting[i] = false
	}
	dmx.Start()
	dmx.outDbg("Init DIMEX!")
	return dmx
}

// ------------------------------------------------------------------------------------
// ------- nucleo do funcionamento
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) Start() {

	go func() {
		for {
			select {
			case dmxR := <-module.Req: // vindo da  aplicação
				if dmxR == ENTER {
					module.outDbg("app pede mx")
					module.handleUponReqEntry() // ENTRADA DO ALGORITMO

				} else if dmxR == EXIT {
					module.outDbg("app libera mx")
					module.handleUponReqExit() // ENTRADA DO ALGORITMO
				} else if dmxR == SNAPSHOT {
					// SNAPSHOT VINDO DA APP
					module.handleSnapshotApp()
				}

			case msgOutro := <-module.Pp2plink.Ind: // vindo de outro processo
				//fmt.Printf("dimex recebe da rede: ", msgOutro)
				if strings.Contains(msgOutro.Message, "respOK") {
					module.outDbg("         <<<---- responde! " + msgOutro.Message)
					module.handleUponDeliverRespOk(msgOutro) // ENTRADA DO ALGORITMO

				} else if strings.Contains(msgOutro.Message, "reqEntry") {
					module.outDbg("          <<<---- pede??  " + msgOutro.Message)
					module.handleUponDeliverReqEntry(msgOutro) // ENTRADA DO ALGORITMO

				} else if strings.Contains(msgOutro.Message, "take snapshot") {
					// SNAPSHOT VINDO OUTRO PROCESSO
					module.handleSnapshotProcesso(msgOutro)
				}

			}
		}
	}()
}

// ------------------------------------------------------------------------------------
// ------- tratamento de pedidos vindos da aplicacao
// ------- UPON ENTRY
// ------- UPON EXIT
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) handleUponReqEntry() {
	/*
					upon event [ dmx, Entry  |  r ]  do
		    			lts.ts++
		    			myTs := lts
		    			resps := 0
		    			para todo processo p
							trigger [ pl , Send | [ reqEntry, r, myTs ]
		    			estado := queroSC
	*/

	module.lcl++
	module.reqTs = module.lcl
	module.nbrResps = 0
	// para todo processo que nao ele mesmo
	for i := 0; i < len(module.addresses); i++ {
		if i != module.id {
			message := fmt.Sprintf("reqEntry, %d, %d", module.id, module.reqTs)
			module.sendToLink(module.addresses[i], message, " ")
		}
	}
	module.st = wantMX

	// fmt.Println("-------------------------")
	// fmt.Println("Enviando reqEntry,", module.id, module.reqTs)
	// fmt.Println("-------------------------")

}

func (module *DIMEX_Module) handleUponReqExit() {
	/*
						upon event [ dmx, Exit  |  r  ]  do
		       				para todo [p, r, ts ] em waiting
		          				trigger [ pl, Send | p , [ respOk, r ]  ]
		    				estado := naoQueroSC
							waiting := {}
	*/

	// Loop sobre a lista waiting, envia respOK para os que estao esperando
	for i, wait := range module.waiting {
		if wait {
			message := fmt.Sprintf("respOK, %d", module.id)
			module.sendToLink(module.addresses[i], message, "")
		}
	}

	module.st = noMX

	// lista de waiting
	// fmt.Println("-------------------------")
	// fmt.Println("Lista Waiting: ", module.waiting)
	// fmt.Println("-------------------------")

	// limpa lista de waiting
	module.waiting = make([]bool, len(module.waiting))

}

// ------------------------------------------------------------------------------------
// ------- tratamento de mensagens de outros processos
// ------- UPON respOK
// ------- UPON reqEntry
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) handleUponDeliverRespOk(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	/*
						upon event [ pl, Deliver | p, [ respOk, r ] ]
		      				resps++
		      				se resps = N
		    				então trigger [ dmx, Deliver | free2Access ]
		  					    estado := estouNaSC

	*/

	// Decifra a mensagem recebida
	var r int
	fmt.Sscanf(msgOutro.Message, "respOK, %d", &r)

	// fmt.Println("-------------------------")
	// fmt.Println("RECEBIDO RESP OK de:", r)
	// fmt.Println("-------------------------")

	// incrementa o nro de respostas recebidas, se igual o numero de processos -1 entra na MX
	module.nbrResps++
	if module.nbrResps == len(module.addresses)-1 {
		module.Ind <- dmxResp{}
		module.st = inMX
	}

}

func (module *DIMEX_Module) handleUponDeliverReqEntry(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	// outro processo quer entrar na SC
	/*
		upon event [ pl, Deliver | p, [ reqEntry, r, rts ]  do
			se (estado == naoQueroSC)   OR
				 (estado == QueroSC AND  myTs >  ts)
				então  trigger [ pl, Send | p , [ respOk, r ]  ]
			senão
				se (estado == estouNaSC) OR
					 (estado == QueroSC AND  myTs < ts)
				então  postergados := postergados + [p, r ]
				lts.ts := max(lts.ts, rts.ts)
	*/

	// Decifra a mensagem recebida
	var r, rts int
	fmt.Sscanf(msgOutro.Message, "reqEntry, %d, %d", &r, &rts)
	//fmt.Printf("Mensagem: reqEntry, %d, %d", r, rts)

	// BEFORE -> Se meu Ts for maior, desempate por Id
	if module.st == noMX || (module.st == wantMX && before(r, rts, module.id, module.reqTs)) {
		// Envia mensagem de resposta OK
		responseMessage := fmt.Sprintf("respOK, %d", module.id)
		module.sendToLink(module.addresses[r], responseMessage, "")
		// BEFORE -> Se meu Ts for menor, desempate por Id
	} else if module.st == inMX || (module.st == wantMX && before(module.id, module.reqTs, r, rts)) {
		// Adiciona o processo à lista de espera
		module.waiting[r] = true
	}

	// Atualiza o relógio lógico local
	if rts > module.lcl {
		module.lcl = rts
	}
}

// ------------------------------------------------------------------------------------
// ------- SNAPSHOT
// ------------------------------------------------------------------------------------

// SNAPSHOT: ESTADO INTERNO, CANAIS DE ENTRADA

// SNAPSHOT RECEBIDO DA APLICACAO
func (module *DIMEX_Module) handleSnapshotApp() {

	// envia mensagem de snapshot para outros
	for i := 0; i < len(module.addresses); i++ {
		if i != module.id {
			module.sendToLink(module.addresses[i], "take snapshot", "")
		}
	}

}

// SNAPSHOT RECEBIDO DE OUTROS PROCESSOS
func (module *DIMEX_Module) handleSnapshotProcesso(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	// Decifra a mensagem recebida
	var r, rts int
	fmt.Sscanf(msgOutro.Message, "take snapshot, %d, %d", &r, &rts)

	// IMPLEMENTAR ALGORITMO SLIDES

	// if not emSnap {
	// 	guarda estrutura dimex em snap[id]
	// 	guarda estado do canal C[origem, esteProcesso] = vazio
	// 	emSnap = true
	// 	idSnap = id
	// }

	//fmt.Printf("Mensagem: take snapshot, %d, %d", r, rts)

	// SNAPSHOT i -> Estado: module.st, module.Ind
	// Escreve SNAPSHOT i em arquivo -> SNAPSHOT module.id

	// Depois de receber segundo "take snapshot" para de gravar o canal

	// // CRIA ARQUIVO DE SNAPSHOT DO PROCESSO
	// filename := fmt.Sprintf(".SNAP%d.txt", module.id)
	// file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	// if err != nil {
	// 	fmt.Println("Error opening file:", err)
	// 	return
	// }
	// defer file.Close() // Ensure the file is closed at the end of the function

	// ESCREVE NO ARQUIVO DO SNAPSHOT

}

// ------------------------------------------------------------------------------------
// ------- funcoes de ajuda
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) sendToLink(address string, content string, space string) {
	module.outDbg(space + " ---->>>>   to: " + address + "     msg: " + content)
	module.Pp2plink.Req <- PP2PLink.PP2PLink_Req_Message{
		To:      address,
		Message: content}
}

func before(oneId, oneTs, othId, othTs int) bool {
	if oneTs < othTs {
		return true
	} else if oneTs > othTs {
		return false
	} else {
		return oneId < othId
	}
}

func (module *DIMEX_Module) outDbg(s string) {
	if module.dbg {
		fmt.Println(". . . . . . . . . . . . [ DIMEX : " + s + " ]")
	}
}
