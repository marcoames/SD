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
	//
	SNAPSHOT
)

type dmxResp struct { // mensagem do módulo DIMEX infrmando que pode acessar - pode ser somente um sinal (vazio)
	// mensagem para aplicacao indicando que pode prosseguir
}

// SNAPSHOT
type StateSNAP struct {
	emSnapshot bool             // se esta em snapshot
	idSnap     int              // snapshot ID
	Canais     map[int][]string // key: sender process ID, value: list of messages recorded
	msgSnap    map[int]bool     // key: sender process ID, value: whether marker received on this channel
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
			emSnapshot: false,
			idSnap:     _snapID,
			Canais:     make(map[int][]string),
			msgSnap:    make(map[int]bool),
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
					fmt.Println("-------------------------")
					fmt.Printf("Mensagem: take snapshot RECEBIDA DA APP")
					fmt.Println("-------------------------")
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
	// If already in snapshot, ignore
	if module.stSNAP.emSnapshot {
		return
	}

	// Mark the snapshot initiation
	module.stSNAP.emSnapshot = true
	module.stSNAP.idSnap++ // Increment snapshot ID

	module.stSNAP.Canais = make(map[int][]string)
	module.stSNAP.msgSnap = make(map[int]bool)

	// module.saveLocalState()

	// Initialize markersReceived for all incoming channels as false
	for i := 0; i < len(module.addresses); i++ {
		if i != module.id {
			module.stSNAP.msgSnap[i] = false
		}
	}

	// Send a "take snapshot com o snapshotID, module.id" message to other processes
	for i := 0; i < len(module.addresses); i++ {
		if i != module.id {
			// responseMessage := fmt.Sprintf("take snapshot, %d", currentSnapID)
			responseMessage := fmt.Sprintf("take snapshot, %d, %d", module.stSNAP.idSnap, module.id)
			module.sendToLink(module.addresses[i], responseMessage, "")

			fmt.Println("-------------------------------------------------")
			fmt.Printf("Mensagem: take snapshot id %d enviada de %d para %d ", module.stSNAP.idSnap, module.id, i)
			fmt.Println("-------------------------------------------------")

		}
	}
}

// SNAPSHOT RECEBIDO DE OUTROS PROCESSOS
func (module *DIMEX_Module) handleSnapshotProcesso(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	// Parse the incoming "take snapshot, snapshotID, senderID" message
	var snapshotID, senderID int
	fmt.Sscanf(msgOutro.Message, "take snapshot, %d, %d", &snapshotID, &senderID)

	fmt.Println("--------------------------------------------------")
	fmt.Printf("Mensagem: take snapshot id %d recebida em %d de %d\n", snapshotID, module.id, senderID)
	fmt.Println("--------------------------------------------------")

	// If in snapshot, store the incoming message
	//if module.stSNAP.emSnapshot {
	module.stSNAP.Canais[senderID] = append(module.stSNAP.Canais[senderID], msgOutro.Message)
	//}

	// If not already in a snapshot, start one
	if !module.stSNAP.emSnapshot {
		module.stSNAP.emSnapshot = true
		module.stSNAP.idSnap = snapshotID
		module.stSNAP.Canais = make(map[int][]string)
		module.stSNAP.msgSnap = make(map[int]bool)

		// Initialize markers for all channels
		for i := 0; i < len(module.addresses); i++ {
			if i != module.id {
				module.stSNAP.msgSnap[i] = false
			}
		}

		// Propagate the snapshot request to other processes
		for i := 0; i < len(module.addresses); i++ {
			if i != module.id {
				responseMessage := fmt.Sprintf("take snapshot, %d, %d", snapshotID, module.id)
				module.sendToLink(module.addresses[i], responseMessage, "")

				fmt.Println("--------------------------------------------------")
				fmt.Printf("Mensagem: take snapshot id %d enviada de %d para %d\n", snapshotID, module.id, i)
				fmt.Println("--------------------------------------------------")
			}
		}

	}

	// If in snapshot, store the incoming message
	// if module.stSNAP.emSnapshot {
	// 	module.stSNAP.Canais[senderID] = append(module.stSNAP.Canais[senderID], msgOutro.Message)
	// }

	if module.stSNAP.emSnapshot && module.stSNAP.idSnap == snapshotID {
		// Already in the snapshot, mark the marker received from the sender
		module.stSNAP.msgSnap[senderID] = true
		fmt.Println("Vetor de mensagem de snap:", module.stSNAP.msgSnap)

		// Check if all markers have been received
		if module.checkSnapshotCompletion() {
			// Save the snapshot since all markers have been received
			module.saveSnapshot(snapshotID)
			module.stSNAP.emSnapshot = false // Reset the snapshot state after saving
		}
	}
}

// Check if all markers have been received for the current snapshot
func (module *DIMEX_Module) checkSnapshotCompletion() bool {
	allMarkersReceived := true

	// Check if markers from all channels have been received
	for i := 0; i < len(module.addresses); i++ {
		if i != module.id && !module.stSNAP.msgSnap[i] {
			allMarkersReceived = false
			break
		}
	}

	if allMarkersReceived {
		fmt.Println("Todos os marcadores recebidos para o snapshot ID:", module.stSNAP.idSnap)
		return true
	} else {
		fmt.Println("Aguardando marcadores de outros processos...")
		return false
	}
}

// Save the snapshot (local state and in-transit messages) to a file
func (module *DIMEX_Module) saveSnapshot(snapshotID int) {
	// Formata o estado local
	stateData := fmt.Sprintf(
		"Snapshot ID: %d\tProcess ID: %d\tProcess State: %v\n",
		snapshotID, // SNAPSHOT ID
		module.id,  // Process ID
		module.st,  // process state
	)

	// Formata as mensagens in-transit
	inTransitData := "In-Transit Messages:\n"
	for sender, msgs := range module.stSNAP.Canais {
		inTransitData += fmt.Sprintf("  From Process %d:\n", sender)
		for _, msg := range msgs {
			inTransitData += fmt.Sprintf("    %s\n", msg)
		}
	}

	// Combine local state and in-transit messages
	fullSnapshot := stateData + inTransitData

	// Define the filename based on the module ID
	fileName := fmt.Sprintf("snapshot_%d.txt", module.id)

	// Create or open the file for appending the snapshot
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening snapshot file:", err)
		return
	}
	defer file.Close()

	// Add a newline before and after the snapshot to ensure proper separation
	fullSnapshot = "\n" + fullSnapshot + "\n-------------------------\n"

	// Write the full snapshot to the file
	_, err = file.WriteString(fullSnapshot)
	if err != nil {
		fmt.Println("Error writing snapshot to file:", err)
		return
	}

	// Log success message
	//module.outDbg(fmt.Sprintf("Snapshot %d saved to %s", snapshotID, fileName))
}

// ------------------------------------------------------------------------------------
// ------- funcoes de ajuda
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) sendToLink(address string, content string, space string) {
	module.outDbg(space + " ---->>>>   to: " + address + "     msg: " + content)

	if module.stSNAP.emSnapshot {
		// Record the message in transit for the snapshot
		module.stSNAP.Canais[module.id] = append(module.stSNAP.Canais[module.id], content)
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
