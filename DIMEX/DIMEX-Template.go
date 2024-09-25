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
	"os"
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
	// Recebe snapshot da App
	SNAPSHOT
)

type dmxResp struct { // mensagem do módulo DIMEX infrmando que pode acessar - pode ser somente um sinal (vazio)
	// mensagem para aplicacao indicando que pode prosseguir
}

// SNAPSHOT
type StateSNAP struct {
	emSnapshot  bool             // se esta em snapshot
	idSnap      int              // snapshot ID
	Canais      map[int][]string // key: senderID, value: lista de mensagens gravadas
	gravaCanais bool             // se deve gravar os canais
	msgSnap     map[int]bool     // key: senderID, value: se o 2 take snapshot foi recebido
}

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

	stSNAP StateSNAP // ESTADO DE SNAPSHOT

	Pp2plink *PP2PLink.PP2PLink // acesso aa comunicacao enviar por PP2PLinq.Req  e receber por PP2PLinq.Ind
}

// ------------------------------------------------------------------------------------
// ------- inicializacao
// ------------------------------------------------------------------------------------

func NewDIMEX(_addresses []string, _id int, _dbg bool, _snapID int) *DIMEX_Module {

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

		stSNAP: StateSNAP{
			emSnapshot:  false,
			idSnap:      _snapID,
			Canais:      make(map[int][]string),
			gravaCanais: false,
			msgSnap:     make(map[int]bool),
		},

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
					// fmt.Println("-------------------------")
					// fmt.Printf("Mensagem: take snapshot RECEBIDA DA APP")
					// fmt.Println("-------------------------")
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
		// Adiciona o processo a lista de espera
		module.waiting[r] = true
	}

	// Atualiza o relogio logico local
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

	if module.stSNAP.emSnapshot {
		return
	}

	// envia take snapshot pra si mesmo
	module.stSNAP.idSnap++ // Increment snapshot ID
	responseMessage := fmt.Sprintf("take snapshot, %d, %d", module.stSNAP.idSnap, module.id)
	module.sendToLink(module.addresses[module.id], responseMessage, "")

}

// SNAPSHOT RECEBIDO DE OUTROS PROCESSOS
func (module *DIMEX_Module) handleSnapshotProcesso(msgOutro PP2PLink.PP2PLink_Ind_Message) {

	// Decifra a mensagem recebida
	var snapshotID, senderID int
	fmt.Sscanf(msgOutro.Message, "take snapshot, %d, %d", &snapshotID, &senderID)

	// Grava o take snapshot recebido
	// if module.stSNAP.emSnapshot && strings.Contains(msgOutro, "take snapshot"){
	// 	module.stSNAP.Canais[senderID] = append(module.stSNAP.Canais[senderID], msgOutro.Message)
	// }

	// Recebe pela 1 vez, nao estava em estado de snapshot
	if !module.stSNAP.emSnapshot {

		module.stSNAP.emSnapshot = true
		module.stSNAP.idSnap = snapshotID
		module.stSNAP.Canais = make(map[int][]string)
		module.stSNAP.gravaCanais = true

		// Salva o estado local em arquivo
		module.saveLocalState()

		// Envia take snapshot para outros processos
		for i := 0; i < len(module.addresses); i++ {
			if i != module.id {
				responseMessage := fmt.Sprintf("take snapshot, %d, %d", snapshotID, module.id)
				module.sendToLink(module.addresses[i], responseMessage, "")
			}
		}

		// Inicializa marcador de 2 take snapshot
		for i := 0; i < len(module.addresses); i++ {
			if i != module.id {
				module.stSNAP.msgSnap[i] = false
			}
		}

	}

	// Já está em snapshot e id do snapshot é o mesmo, 2 take snapshot recebido
	if module.stSNAP.emSnapshot && module.stSNAP.idSnap == snapshotID {
		module.stSNAP.msgSnap[senderID] = true
		module.stSNAP.gravaCanais = false
		if module.checkSnapshotCompletion() {
			// Salva snapshot completo
			module.stSNAP.emSnapshot = false // Sai do estado de snapshot
			module.saveSnapshot()
			// fmt.Printf("\n")
			// fmt.Println("--------------------------------------")
			// fmt.Printf("SNAPSHOT NUMERO: %d ESCRITO", snapshotID)
			// fmt.Println("--------------------------------------")
			// fmt.Printf("\n")

			module.write_snapnumber(snapshotID)

		}
	}

}

func (module *DIMEX_Module) write_snapnumber(snapshotID int) {
	// Nome do arquivo com id do processo
	fileName := "snapshots.txt"

	file, err := os.OpenFile(fileName, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening snapshot file:", err)
		return
	}
	defer file.Close()

	data := fmt.Sprintf("snapshots salvos: %d", snapshotID)

	// Escreve o numero do snapshot salvo no arquivo
	_, err = file.WriteString(data)
	if err != nil {
		fmt.Println("Error writing snapshot to file:", err)
		return
	}
}

func (module *DIMEX_Module) checkSnapshotCompletion() bool {
	// Se 2 take snapshot foi recebido por todos os canais
	for i := 0; i < len(module.addresses); i++ {
		if i != module.id && !module.stSNAP.msgSnap[i] {
			fmt.Println("Aguardando marcadores de outros processos...")
			return false
		}
	}
	fmt.Println("Todos os marcadores recebidos para o snapshot ID:", module.stSNAP.idSnap)
	return true
}

func (module *DIMEX_Module) saveLocalState() {
	// Formata o estado local
	stateData := fmt.Sprintf(
		"Snapshot ID: %d\tProcess ID: %d\t Logic Clock: %d\tWaiting: %v\tProcess State: %v",
		module.stSNAP.idSnap, // SNAPSHOT ID
		module.id,            // Process ID
		module.lcl,           // process lcl
		module.waiting,       // waiting array
		module.st,            // process state
	)

	// Nome do arquivo com id do processo
	fileName := fmt.Sprintf("snapshot_%d.txt", module.id)

	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening snapshot file:", err)
		return
	}
	defer file.Close()

	// Escreve estado local no arquivo
	_, err = file.WriteString(stateData)
	if err != nil {
		fmt.Println("Error writing snapshot to file:", err)
		return
	}

}

func (module *DIMEX_Module) saveSnapshot() {

	// Formata as mensagens do canal
	channelMsg := "Channel Messages:\n"

	for sender, msgs := range module.stSNAP.Canais {
		channelMsg += fmt.Sprintf("Canal[%d, %d] = %v\n", module.id, sender, msgs)
	}

	// Nome do arquivo com id do processo
	fileName := fmt.Sprintf("snapshot_%d.txt", module.id)

	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening snapshot file:", err)
		return
	}
	defer file.Close()

	channelMsg = "\n" + channelMsg + "\n-------------------------\n"

	// Escreve msg do canal no arquivo
	_, err = file.WriteString(channelMsg)
	if err != nil {
		fmt.Println("Error writing snapshot to file:", err)
		return
	}
}

// ------------------------------------------------------------------------------------
// ------- funcoes de ajuda
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) sendToLink(address string, content string, space string) {
	module.outDbg(space + " ---->>>>   to: " + address + "     msg: " + content)

	// Se esta em snapshot grava o canal, nao grava as mensagens de take snapshot
	if module.stSNAP.emSnapshot && !(strings.Contains(content, "take snapshot")) {
		var senderID int
		for idx, addr := range module.addresses {
			if addr == address {
				senderID = idx
				break
			}
		}
		module.stSNAP.Canais[senderID] = append(module.stSNAP.Canais[senderID], content)
	}

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
